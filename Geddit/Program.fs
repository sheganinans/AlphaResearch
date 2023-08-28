open System
open System.IO

open FSharp.Collections.ParallelSeq
open Geddit
open Geddit.ThetaData
open Geddit.Discord

let thetaData = Theta ()

Async.Sleep 5_000 |> Async.RunSynchronously

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let finishedMailbox = MailboxProcessor.Start (fun inbox ->
  use sw = File.AppendText "finished.txt"
  async {
    let! (root : string) = inbox.Receive ()
    sw.WriteLine root
    sw.Flush ()
  })

let finished =
  try File.ReadLines "finished.txt" |> Set.ofSeq
  with _ -> Set.empty

let go () =
  match Roots.getStockRoots () with
  | Error err -> printfn $"{err}"
  | Result.Ok roots ->
    roots
    |> Seq.filter (not << finished.Contains)
    |> Seq.iter (fun root ->
      Directory.CreateDirectory root |> ignore
      discord.SendNotification $"starting: {root}" |> Async.Start
      let mutable trySet =
        seq { 0..(endDay-startDay).Days }
        |> Seq.map (startDay.AddDays << float)
        |> Set.ofSeq

      let mutable noData = []
        
      while trySet.Count <> 0 do
        printfn $"dates left: {trySet.Count}"
        let res =
          trySet
          |> PSeq.withDegreeOfParallelism 8
          |> PSeq.map (fun day ->
            printfn $"req: {root} {day}"
            day, StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously)
          |> PSeq.cache
        let data = res |> PSeq.filter (function | _, RspStatus.Ok _ -> true | _ -> false)
        let discons = res |> PSeq.filter (fun (_, r) -> r = RspStatus.Disconnected)
        let errs = res |> PSeq.filter (function | _, RspStatus.Err _ -> true | _ -> false)
        async {
          data |> PSeq.iter
            (function
            | day, RspStatus.Ok data ->
              printfn $"save: {root} {day}"
              StockTradeQuotes.saveData root day data
            | _ -> raise (Exception "UNEXP1: This should never happen."))
        } |> Async.Start

        noData <- List.append noData (res |> PSeq.toList |> List.filter (fun (_, r) -> r = RspStatus.NoData))
        trySet <- discons |> PSeq.map fst |> Set.ofSeq |> Set.union (errs |> Seq.map fst |> Set.ofSeq)
        
        if trySet.Count <> 0
        then
          discord.SendNotification "errs found, restarting thetadata." |> Async.Start
          thetaData.Reset ()
          Async.Sleep 3000 |> Async.RunSynchronously
      
      (
        let noDataFile = $"{root}.nodata.txt"
        use sw = new StreamWriter (noDataFile)
        noData |> List.iter (fun d -> sw.WriteLine (d.ToString ()))
        sw.Close ()
        Wasabi.uploadFile noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
        File.Delete noDataFile
      )
    
      async {
        while (Directory.GetFiles root).Length <> 0 do do! Async.Sleep 100
        finishedMailbox.Post root
        Directory.Delete root
      } |> Async.Start)
    
try go ()
with err ->
  printfn $"{err}"
  thetaData.Kill ()
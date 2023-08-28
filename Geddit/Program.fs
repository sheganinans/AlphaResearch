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

let TOTAL_DAYS = (endDay-startDay).Days

type private SyncFinished = class end

let finishedMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    while true do
      let! (root : string) = inbox.Receive ()
      lock typeof<SyncFinished> (fun () ->
        use sw = File.AppendText "finished.txt"
        sw.WriteLine root
        sw.Flush ())
      Directory.Delete root
  })

type private SyncCounter = class end

let counterMailbox = MailboxProcessor.Start (fun inbox ->
  let mutable m = Map.empty
  async {
    while true do
      let! (root : string) = inbox.Receive ()
      lock typeof<SyncCounter> (fun () ->
        m <- m |> Map.change root (function | None -> Some 1 | Some n -> Some (n + 1))
        let c = m |> Map.find root
        printfn $"c: {c}, {TOTAL_DAYS}"
        if TOTAL_DAYS = c
        then finishedMailbox.Post root)
    }
  )

let finished =
  try File.ReadLines "finished.txt" |> Set.ofSeq
  with _ -> Set.empty

let go () =
  match Roots.getStockRoots () with
  | Error err -> printfn $"{err}"
  | Result.Ok roots ->
    roots
    |> Seq.filter (not << finished.Contains)
    |> PSeq.withDegreeOfParallelism 8
    |> PSeq.iter (fun root ->
      Directory.CreateDirectory root |> ignore
      discord.SendNotification $"starting: {root}" |> Async.Start
      let mutable trySet =
        seq { 0..(endDay-startDay).Days - 1 }
        |> Seq.map (startDay.AddDays << float)
        |> Set.ofSeq

      let mutable noDataAcc = []
        
      while trySet.Count <> 0 do
        let res =
          trySet
          |> Seq.map (fun day -> day, StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously)
          |> Seq.cache
        let data = res |> Seq.filter (function | _, RspStatus.Ok _ -> true | _ -> false)
        let discons = res |> Seq.filter (fun (_, r) -> r = RspStatus.Disconnected)
        let errs = res |> Seq.filter (function | _, RspStatus.Err _ -> true | _ -> false)
        let noData = res |> Seq.filter (fun (_, r) -> r = RspStatus.NoData)
        data |> Seq.iter
          (function
          | day, RspStatus.Ok data ->
            counterMailbox.Post root
            StockTradeQuotes.saveData root day data
          | _ -> raise (Exception "UNEXP1: This should never happen."))

        noDataAcc <- List.append noDataAcc (noData |> List.ofSeq)
        trySet <- discons |> Seq.map fst |> Set.ofSeq |> Set.union (errs |> Seq.map fst |> Set.ofSeq)
        
        if trySet.Count <> 0
        then
          discord.SendNotification "errs found, restarting thetadata." |> Async.Start
          thetaData.Reset ()
          Async.Sleep 3000 |> Async.RunSynchronously
      
      (
        let noDataFile = $"{root}.nodata.txt"
        use sw = new StreamWriter (noDataFile)
        noDataAcc |> List.iter (fun d ->
          counterMailbox.Post root
          sw.WriteLine (d.ToString ()))
        sw.Close ()
        Wasabi.uploadFile noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
        File.Delete noDataFile
      ))
    
try go ()
with err ->
  printfn $"{err}"
  thetaData.Kill ()
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

let finishedMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    while true do
      let! (root : string) = inbox.Receive ()
      use sw = File.AppendText "finished.txt"
      sw.WriteLine root
      sw.Flush ()
      Directory.Delete root
  })

let counterMailbox = MailboxProcessor.Start (fun inbox ->
  let mutable m = Map.empty
  async {
    while true do
      let! ((root, day) : string * DateTime) = inbox.Receive ()
      m <- m |> Map.change root (function | None -> Some 1 | Some n -> Some (n + 1))
      let c = m |> Map.find root
      printfn $"{root}: %02.2f{100. * (float c / float TOTAL_DAYS)}%% %04i{day.Year}-%02i{day.Month}-%02i{day.Day}"
      if TOTAL_DAYS = c
      then finishedMailbox.Post root
    }
  )

let finished =
  try File.ReadLines "finished.txt" |> Set.ofSeq
  with _ -> Set.empty


type private SyncGo = class end

let go () =
  match Roots.getStockRoots () with
  | Error err -> printfn $"{err}"
  | Result.Ok roots ->
    roots
    |> Seq.map (fun root -> root.Replace ('/', '.'))
    |> Seq.filter (not << finished.Contains)
    |> Seq.iter (fun root ->
      Directory.CreateDirectory root |> ignore
      discord.SendNotification $"starting: {root}" |> Async.Start
      let mutable trySet =
        seq { 0..(endDay-startDay).Days - 1 }
        |> Seq.map (startDay.AddDays << float)
        |> Set.ofSeq

      let mutable noDataAcc = []
      let mutable disconns = []
      let mutable errors = []
        
      while trySet.Count <> 0 do
        trySet
        |> PSeq.withDegreeOfParallelism 6
        |> PSeq.iter (fun day ->
          match StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously with
          | RspStatus.Disconnected -> lock typeof<SyncGo> (fun () ->
            printfn "disconnected!"
            thetaData.Reset ()
            disconns <- day:: disconns)
          | RspStatus.NoData -> lock typeof<SyncGo> (fun () ->
            counterMailbox.Post (root, day)
            noDataAcc <- day :: noDataAcc)
          | RspStatus.Err err -> lock typeof<SyncGo> (fun () ->
            printfn $"{err}"
            errors <- day :: errors)
          | RspStatus.Ok data ->
            counterMailbox.Post (root, day)
            StockTradeQuotes.saveData root day data)

        trySet <- disconns |> Set.ofList |> Set.union (errors |> Set.ofList)
          
      (
        let noDataFile = $"{root}.nodata.txt"
        use sw = new StreamWriter (noDataFile)
        noDataAcc |> List.iter (fun d -> sw.WriteLine (d.ToString ()))
        sw.Close ()
        Wasabi.uploadFile noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
        File.Delete noDataFile
      ))
    
try go ()
with err ->
  printfn $"{err}"
  thetaData.Kill ()
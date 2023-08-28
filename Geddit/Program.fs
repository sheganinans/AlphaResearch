open System
open System.IO

open FSharp.Collections.ParallelSeq
open Geddit
open Geddit.ThetaData
open Geddit.Discord

let thetaData = Theta ()

Async.Sleep 3000 |> Async.RunSynchronously

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

match Roots.getStockRoots () with
| Error err -> printfn $"{err}"
| Result.Ok roots ->
  roots
  |> Seq.iter (fun root ->
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
        |> PSeq.map (fun day -> day, StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously)
      let data = res |> PSeq.filter (function | _, RspStatus.Ok _ -> true | _ -> false)
      let discons = res |> PSeq.filter (fun (_, r) -> r = RspStatus.Disconnected)
      let errs = res |> PSeq.filter (function | _, RspStatus.Err _ -> true | _ -> false)
      async {
        data |> PSeq.iter
          (function
          | day, RspStatus.Ok data ->
            printfn $"saving, {root}: {day}"
            StockTradeQuotes.saveData root day data
          | _ -> raise (Exception "UNEXP1: This should never happen."))
      } |> Async.Start

      noData <- List.append noData (res |> Seq.toList |> List.filter (fun (_, r) -> r = RspStatus.NoData))
      trySet <- discons |> Seq.map fst |> Set.ofSeq |> Set.union (errs |> Seq.map fst |> Set.ofSeq)
      
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
    use sw = File.AppendText "finished.txt"
    sw.WriteLine root
    sw.Close ())
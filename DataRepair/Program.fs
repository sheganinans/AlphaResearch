open System
open System.Collections.Concurrent
open System.IO

open Amazon.S3.Model

open Shared
open Shared.Discord
open Shared.ThetaData

let BUCKET = StockTradeQuotes.BUCKET

// for o in Wasabi.getWasabiObjs BUCKET "" do
//   if o.Key.Split('/').Length = 3
//   then
//     printfn $"{o.Key}"
//     Wasabi.deleteFile BUCKET o.Key |> ignore

let chunked () =
  let mutable currSym = ""
  let mutable acc = []
  let objToSym (o : S3Object) = o.Key.Split('/')[0]
  seq {
    for o in Wasabi.getWasabiObjs BUCKET "" do
      if currSym = "" then currSym <- objToSym o
      if currSym <> objToSym o
      then
        yield acc
        currSym <- objToSym o
        acc <- [o.Key]
      else
        acc <- o.Key :: acc
  }

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let mutable good = 0
let mutable bad = 0

let HTML_CONCURRENCY = 16

let thetaData = Theta ()

chunked ()
|> Seq.iter (fun job ->
  let root = job[0].Split('/')[0]
  let mutable noData = 
    match job |> List.tryFind (fun s -> s.Contains "nodata.txt") with
    | None -> Set.empty
    | Some f ->
      Wasabi.downloadFile f BUCKET f
      File.ReadLines f
      |> Seq.map (fun s -> try Some <| DateTime.Parse s with _ -> None)
      |> Seq.choose id
      |> Set.ofSeq 
  let job =
    job
    |> List.filter (fun s -> (not <| s.Contains "nodata.txt") && (not <| s.Contains ".err"))
    |> List.map (fun s ->
      try
        let d = ((s.Split('/')[1]).Split('.')[0]).Split '-' |> Array.map int
        Some <| DateTime (d[0], d[1], d[2])
      with _ -> None)
    |> List.choose id
    |> Set.ofList
  if job.Count + noData.Count = 2039
  then
    good <- good + 1
    discord.SendNotification $"{root}: all good." |> Async.Start
  else
    discord.SendNotification $"{root}: requires fix" |> Async.Start
    bad <- bad + 1
    let s = ConcurrentDictionary<DateTime, unit> ()
    seq { 0..(endDay-startDay).Days }
    |> Seq.map (startDay.AddDays << float)
    |> Seq.filter (fun day -> (not <| noData.Contains day) && (not <| job.Contains day))
    |> Seq.iter (fun day ->
      s.TryAdd (day, ()) |> ignore
      while s.Count > HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
      async {
        let mutable retry = true
        let inline finishedSuccessfully () =
          s.TryRemove day |> ignore
          retry <- false
        match! StockTradeQuotes.reqAndConcat root day with
        | RspStatus.Err err -> discord.SendAlert $"repair1: {err}" |> Async.Start
        | RspStatus.Disconnected ->
          thetaData.Reset ()
          do! Async.Sleep 10_000
        | RspStatus.NoData ->
          finishedSuccessfully ()
        | RspStatus.Ok data ->
          FileOps.saveData (SecurityDescrip.Stock (root, day)) data
          finishedSuccessfully ()
      } |> Async.Start)
  discord.SendNotification $"perc good: {100. * (float good / float (good + bad))}" |> Async.Start)
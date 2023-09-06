open System
open System.IO
open System.Collections.Concurrent

open Microsoft.FSharp.Control
open Shared
open Shared.Roots
open Shared.ThetaData
open Shared.Discord

let thetaData = Theta ()

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let TOTAL_DAYS = (endDay-startDay).Days

let finishedSet =
  try
    File.ReadLines "finished.txt"
    |> Seq.map (fun s -> try Some (DateTime.Parse s) with _ -> None)
    |> Seq.choose id
    |> Set.ofSeq
  with _ -> Set.empty

if not <| Directory.Exists "data" then Directory.CreateDirectory "data" |> ignore

type private SyncCount = class end
let CHUNK_COUNT = 1000

Async.Sleep 7000 |> Async.RunSynchronously

let HTML_CONCURRENCY = 16

seq { 0..(endDay-startDay).Days - 1 }
|> Seq.map (startDay.AddDays << float)
|> Seq.filter (not << finishedSet.Contains)
|> Seq.map (fun day -> {| Day = day; Contracts = getContracts day |})
|> Seq.iter (fun r ->
  discord.SendNotification $"starting: {r.Day}" |> Async.Start
  let mutable p = 0
  match r.Contracts with
  | Error e -> discord.SendAlert e |> Async.Start
  | Result.Ok cs ->
    match cs with
    | ContractRes.NoData -> ()
    | HasData cs ->
      cs
      |> Seq.chunkBySize CHUNK_COUNT
      |> Seq.iter (fun chunk ->
        let mutable n = 0
        let s = ConcurrentDictionary<OptionDescrip, unit> ()
        for c in chunk do
          s.TryAdd (c, ()) |> ignore
          while s.Count > HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
          async {
            let mutable retry = true
            while retry do
              match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
              | RspStatus.Err err ->
                discord.SendAlert $"getContract1: {err}" |> Async.Start
              | RspStatus.Disconnected ->
                thetaData.Reset ()
              | RspStatus.NoData ->
                s.TryRemove c |> ignore
                retry <- false
                lock typeof<SyncCount> (fun () -> n <- n + 1)
              | RspStatus.Ok data ->
                  try
                    FileOps.saveData (SecurityDescrip.Option c) data
                    s.TryRemove c |> ignore
                    retry <- false
                    lock typeof<SyncCount> (fun () -> n <- n + 1)
                  with err ->
                    discord.SendAlert $"getContract2: {err}" |> Async.Start
          } |> Async.Start
        while lock typeof<SyncCount> (fun () -> n < chunk.Length) do
          printfn $"sleep, {r.Day}: %0.2f{100. * (float p / float cs.Length)}%%"
          Async.Sleep 10 |> Async.RunSynchronously
        use sw = File.AppendText "finished.txt" in sw.WriteLine (r.Day.ToString ())
        p <- p + n
        discord.SendNotification $"{r.Day}: %0.2f{100. * (float p / float cs.Length)}%%" |> Async.Start))

discord.SendAlert "done!" |> Async.RunSynchronously

Async.Sleep 3000 |> Async.RunSynchronously
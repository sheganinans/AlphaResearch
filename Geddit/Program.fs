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
  try File.ReadLines "finished.txt" |> Seq.map DateTime.Parse |> Set.ofSeq
  with _ -> Set.empty

if not <| Directory.Exists "data" then Directory.CreateDirectory "data" |> ignore

type private SyncCount = class end
let CHUNK_COUNT = 5_000

let withChunking chunk chunkBy f =
  chunk
    |> Seq.chunkBySize chunkBy
    |> Seq.iter f

Async.Sleep 7000 |> Async.RunSynchronously

let HTML_CONCURRENCY = 40

seq { 0..(endDay-startDay).Days - 1 }
|> Seq.map (startDay.AddDays << float)
|> Seq.filter (not << finishedSet.Contains)
|> Seq.map (fun day -> {| Day = day; Contracts = getContracts day |})
|> Seq.iter (fun r ->
  discord.SendNotification $"starting: {r.Day}" |> Async.Start
  match r.Contracts with
  | Error e -> discord.SendAlert e |> Async.Start
  | Result.Ok cs ->
    match cs with
    | ContractRes.NoData -> ()
    | HasData cs ->
      let mutable i = 0
      withChunking cs CHUNK_COUNT (fun chunk ->
        i <- i + 1
        let mutable n = 0
        let s = ConcurrentDictionary<OptionDescrip, unit> ()
        for c in chunk do
          s.TryAdd (c, ()) |> ignore
          while s.Count >= HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
          async {
            let mutable retry = true
            while retry do
              match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
              | RspStatus.Err err ->
                discord.SendAlert $"getContract1: {err}" |> Async.Start
              | RspStatus.Disconnected ->
                thetaData.Reset ()
                Async.Sleep 1000 |> Async.RunSynchronously
              | RspStatus.NoData -> retry <- false
              | RspStatus.Ok data ->
                  try
                    FileOps.saveData (SecurityDescrip.Option c) data
                    s.TryRemove c |> ignore
                    retry <- false
                  with err ->
                    discord.SendAlert $"getContract2: {err}" |> Async.Start
                    Async.Sleep 1000 |> Async.RunSynchronously
            lock typeof<SyncCount> (fun () -> n <- n + 1)
          } |> Async.Start
        while lock typeof<SyncCount> (fun () -> n < chunk.Length) do
          printfn $"sleep: {n} {chunk.Length} %0.2f{100. * (float n / float chunk.Length)}%%"
          Async.Sleep 10 |> Async.RunSynchronously
        discord.SendNotification $"{r.Day}: %0.2f{100. * (float i * (float CHUNK_COUNT / float cs.Length))}%%." |> Async.Start))

discord.SendAlert "done!" |> Async.RunSynchronously

Async.Sleep 3000 |> Async.RunSynchronously
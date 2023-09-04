open System
open System.IO

open FSharp.Collections.ParallelSeq

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

let CHUNK_COUNT = 5000

Async.Sleep 7000 |> Async.RunSynchronously
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
      let mutable trySet = cs |> Set.ofSeq
      while trySet.Count <> 0 do
        trySet <-
          cs
          |> Array.chunkBySize CHUNK_COUNT
          |> (Set.empty |> Array.fold (fun retries chunk ->
              let mutable acc = 0
              let ret =
                retries |> Set.union
                  (chunk
                    |> PSeq.withDegreeOfParallelism 16
                    |> (Set.empty |> PSeq.fold (fun retries c ->
                      match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
                      | RspStatus.Err err ->
                        discord.SendAlert $"getContract1: {err}" |> Async.Start
                        lock typeof<SyncCount> (fun () -> acc <- acc + 1)
                        retries.Add c
                      | RspStatus.Disconnected ->
                        thetaData.Reset ()
                        lock typeof<SyncCount> (fun () -> acc <- acc + 1)
                        retries.Add c
                      | RspStatus.NoData ->
                        lock typeof<SyncCount> (fun () -> acc <- acc + 1)
                        retries
                      | RspStatus.Ok data ->
                        async {
                          try FileOps.saveData (SecurityDescrip.Option c) data
                          with err ->
                            discord.SendAlert $"getContract2: {err}" |> Async.Start
                          lock typeof<SyncCount> (fun () -> acc <- acc + 1)
                        } |> Async.Start
                        retries)))
              let mutable sleep = true
              while sleep do
                Async.Sleep 10 |> Async.RunSynchronously
                lock typeof<SyncCount> (fun () -> sleep <- acc <> CHUNK_COUNT)
              ret))
        if trySet.Count <> 0
        then
          Async.Sleep 20_000 |> Async.RunSynchronously
          discord.SendAlert $"restarting {r.Day} with {trySet.Count} saved dates" |> Async.Start)

discord.SendAlert "done!" |> Async.RunSynchronously

Async.Sleep 3000 |> Async.RunSynchronously
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
          |> Array.chunkBySize ((cs.Length / 4) / 4)
          |> Array.map (fun arr ->
            arr |> Array.chunkBySize (((cs.Length / 4) / 4) / 4))
          |> Array.map (fun arr ->
            arr |> Array.map (fun arr -> arr |> Array.chunkBySize ((((cs.Length / 4) / 4) / 4) / 4)))
          |> PSeq.withDegreeOfParallelism Environment.ProcessorCount
          |> (Set.empty |> PSeq.fold (fun retries chunk ->
            retries |> Set.union
              (chunk
              |> PSeq.withDegreeOfParallelism Environment.ProcessorCount
              |> (Set.empty |> PSeq.fold (fun retries chunk ->
                retries |> Set.union
                  (chunk
                  |> PSeq.withDegreeOfParallelism Environment.ProcessorCount
                  |> (Set.empty |> PSeq.fold (fun retries chunk ->
                      retries |> Set.union
                        (chunk
                        |> PSeq.withDegreeOfParallelism Environment.ProcessorCount
                        |> (Set.empty |> PSeq.fold (fun retries c ->
                            match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
                            | RspStatus.Err err ->
                              discord.SendAlert $"getContract1: {err}" |> Async.Start
                              retries.Add c
                            | RspStatus.Disconnected ->
                              thetaData.Reset ()
                              retries.Add c
                            | RspStatus.NoData -> retries
                            | RspStatus.Ok data ->
                              try
                                if not <| Directory.Exists $"data/{c.Root}" then Directory.CreateDirectory $"data/{c.Root}" |> ignore
                                FileOps.saveData (SecurityDescrip.Option c) data
                                let f = FileOps.toFileName (SecurityDescrip.Option c)
                                Wasabi.uploadPath $"data/{f}" OptionTradeQuotes.BUCKET f
                                File.Delete $"data/{f}"
                                retries
                              with err ->
                                discord.SendAlert $"getContract2: {err}" |> Async.Start
                                retries.Add c)))))))))))
        if trySet.Count <> 0
        then
          Async.Sleep 20_000 |> Async.RunSynchronously
          discord.SendAlert $"restarting {r.Day} with {trySet.Count} saved dates" |> Async.Start)

discord.SendAlert "done!" |> Async.RunSynchronously

Async.Sleep 3000 |> Async.RunSynchronously
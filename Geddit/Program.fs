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
  let CHUNK_COUNT = 50_000

  let withChunking retries chunk chunkBy f =
    retries |> Set.union
      (chunk
        |> Seq.chunkBySize chunkBy
        |> (Set.empty |> Seq.fold (fun retries chunk -> retries |> Set.union (f chunk))))

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
        let CHUNK_PERC = 0.01
        let mutable trySet = cs |> Set.ofSeq
        while trySet.Count <> 0 do
          let mutable p = 0
          trySet <-
            withChunking trySet cs (int <| float cs.Length * CHUNK_PERC) (fun chunk ->
              p <- p + (int <| CHUNK_PERC * 100.)
              let mutable n = 0
              let r = 
                let s = ConcurrentDictionary<OptionDescrip, unit> ()
                let retries = ConcurrentDictionary<OptionDescrip, unit> ()
                for c in chunk do
                  s.TryAdd (c, ()) |> ignore
                  while s.Count > 40 do Async.Sleep 10 |> Async.RunSynchronously
                  async {
                    match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
                    | RspStatus.Err err ->
                      discord.SendAlert $"getContract1: {err}" |> Async.Start
                      retries.TryAdd (c, ()) |> ignore
                    | RspStatus.Disconnected ->
                      thetaData.Reset ()
                      retries.TryAdd (c, ()) |> ignore
                    | RspStatus.NoData -> ()
                    | RspStatus.Ok data ->
                        try
                          FileOps.saveData (SecurityDescrip.Option c) data
                          s.TryRemove c |> ignore
                        with err ->
                          retries.TryAdd (c, ()) |> ignore
                          discord.SendAlert $"getContract2: {err}" |> Async.Start
                    lock typeof<SyncCount> (fun () -> n <- n + 1)
                  } |> Async.Start
                discord.SendNotification $"{r.Day}: {p}%%." |> Async.Start
                retries
              while lock typeof<SyncCount> (fun () -> n < trySet.Count) do
                Async.Sleep 10 |> Async.RunSynchronously
              if r.Count > 0 then p <- 0
              r |> Seq.map (fun kv ->  kv.Key) |> Set.ofSeq)
          if trySet.Count <> 0
          then
            Async.Sleep 20_000 |> Async.RunSynchronously
            discord.SendAlert $"restarting {r.Day} with {trySet.Count} saved dates" |> Async.Start)

  discord.SendAlert "done!" |> Async.RunSynchronously

  Async.Sleep 3000 |> Async.RunSynchronously
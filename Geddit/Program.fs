open System
open System.IO
open System.Collections.Concurrent

open Microsoft.FSharp.Control
open Shared
open Shared.Roots
open Shared.ThetaData
open Shared.Discord

let thetaData = Theta ()

let startDay = DateTime (2020, 06, 26)
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
let CHUNK_COUNT = 5000

Async.Sleep 7000 |> Async.RunSynchronously

let HTML_CONCURRENCY = 40

type Counter =
  | NewDay of DateTime * int
  | Incr

let counter = MailboxProcessor.Start (fun inbox ->
  let mutable day = DateTime.Now
  let mutable i = 0
  let mutable tot = 0
  async {
    while true do
      match! inbox.Receive () with
      | NewDay (d, t) ->
        i <- 0
        day <- d
        tot <- t
      | Incr -> i <- i + 1
      if i % 5000 = 0 then discord.SendNotification $"{day}: %0.2f{100. * (float i / float tot)}%%" |> Async.Start
  })

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
      let s = ConcurrentDictionary<OptionDescrip, unit> ()
      counter.Post (NewDay (r.Day, cs.Length))
      cs
      |> Array.iter (fun c ->
        s.TryAdd (c, ()) |> ignore
        while s.Count > HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
        async {
          let mutable retry = true
          let inline finishedSuccessfully () =
            counter.Post Incr
            s.TryRemove c |> ignore
            retry <- false
          while retry do
            match OptionTradeQuotes.reqAndConcat (SecurityDescrip.Option c) |> Async.RunSynchronously with
            | RspStatus.Err err ->
              if err.ErrType = "PERMISSION" then finishedSuccessfully ()
              discord.SendAlert $"getContract1:\n{c}\n{err}" |> Async.Start
            | RspStatus.Disconnected ->
              thetaData.Reset ()
              do! Async.Sleep 10_000
            | RspStatus.NoData -> finishedSuccessfully ()
            | RspStatus.Ok data ->
                try
                  if not <| Directory.Exists $"data/{c.Root}" then Directory.CreateDirectory $"data/{c.Root}" |> ignore
                  if not <| Directory.Exists $"data/{c.Root}/%04i{c.Day.Year}%02i{c.Day.Month}%02i{c.Day.Day}"
                  then Directory.CreateDirectory $"data/{c.Root}/%04i{c.Day.Year}%02i{c.Day.Month}%02i{c.Day.Day}" |> ignore
                  FileOps.saveData (SecurityDescrip.Option c) OptionTradeQuotes.BUCKET data
                  finishedSuccessfully ()
                with err -> discord.SendAlert $"getContract2: {err}" |> Async.Start
        } |> Async.Start)
    use sw = File.AppendText "finished.txt" in sw.WriteLine (r.Day.ToString ()))

discord.SendAlert "done!" |> Async.RunSynchronously

Async.Sleep 3000 |> Async.RunSynchronously
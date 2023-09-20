open System
open System.Collections.Concurrent
open System.IO

open Shared
open Shared.Discord
open Shared.ThetaData

let BUCKET = StockTradeQuotes.BUCKET

let s = ConcurrentDictionary<string * DateTime, unit> ()

let HTML_CONCURRENCY = 8

let thetaData = Theta ()

File.ReadLines "./errs.txt"
|> Seq.iter (fun l ->
  let f = l.Split '/'
  let root, f = f[0], f[1]
  let ds = (f.Split('.')[0]).Split '-' |> Array.map int
  let day = DateTime (ds[0], ds[1], ds[2])
  s.TryAdd ((root, day), ()) |> ignore
  while s.Count > HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
  printfn $"{l}"
  async {
    match! StockTradeQuotes.reqAndConcat root day with
    | RspStatus.Err err -> discord.SendAlert $"repair1: {err}" |> Async.Start
    | RspStatus.Disconnected ->
      thetaData.Reset ()
      do! Async.Sleep 10_000
    | RspStatus.NoData -> s.TryRemove ((root, day)) |> ignore
    | RspStatus.Ok data ->
      FileOps.saveData (SecurityDescrip.Stock (root, day)) BUCKET data
      s.TryRemove ((root, day)) |> ignore
  } |> Async.Start)

printfn "done!"
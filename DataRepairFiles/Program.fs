open System
open System.Collections.Concurrent
open System.IO

open FSharp.Collections.ParallelSeq
open Shared
open Shared.Discord
open Shared.ThetaData

let BUCKET = StockTradeQuotes.BUCKET

let thetaData = Theta ()

File.ReadLines "./errs.txt"
|> PSeq.iter (fun l ->
  let f = l.Split '/'
  let root, f = f[0], f[1]
  let ds = (f.Split('.')[0]).Split '-' |> Array.map int
  let day = DateTime (ds[0], ds[1], ds[2])
  if not <| Directory.Exists root then Directory.CreateDirectory root |> ignore
  printfn $"{l}"
  let mutable retry = true
  while retry do
    try
      match StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously with
      | RspStatus.Err err -> discord.SendAlert $"repair1: {err}" |> Async.Start
      | RspStatus.Disconnected ->
        thetaData.Reset ()
        Async.Sleep 10_000 |> Async.RunSynchronously
      | RspStatus.NoData -> retry <- false
      | RspStatus.Ok data ->
        FileOps.saveData (SecurityDescrip.Stock (root, day)) BUCKET data
        retry <- false
    with err -> printfn $"{err}")

printfn "done!"
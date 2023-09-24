open System
open ClickHouse.Client.Copy
open Dapper

open FSharp.Collections.ParallelSeq
open Shared.Clickhouse

let logRet () =
  let i = new ClickHouseBulkCopy (ck)
  i.DestinationTableName <- "quote_1min_agg"
  i.BatchSize <- 100_000
  getUniqTickers ()
  |> Seq.iter (fun t ->
    printfn $"{t}"
    getLogReturns t 1 Minute
    |> Seq.map (fun r -> [| box r.ticker; box r.ts; box r.log_ret |])
    |> i.WriteToServerAsync
    |> Async.AwaitTask
    |> Async.RunSynchronously)

let count () =
  let i = new ClickHouseBulkCopy (ck)
  i.DestinationTableName <- "ticker_count"
  i.BatchSize <- 1
  getUniqTickers ()
  |> Seq.iter (fun t ->
    printfn $"{t}"
    let count = 
      ck.Query<uint64> $"select count() from thetadata_stock_trade_quotes prewhere ticker == '{t}'"
      |> Seq.head
    [|[| box t; box count |]|]
    |> i.WriteToServerAsync
    |> Async.AwaitTask
    |> Async.RunSynchronously)

//count ()

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let ds =
  seq { 0..(endDay-startDay).Days - 1 }
  |> Seq.map (startDay.AddDays << float)

let countByDay () =
  let i = new ClickHouseBulkCopy (ck)
  i.DestinationTableName <- "ticker_count_by_day"
  i.BatchSize <- 1000
  getUniqTickers ()
  |> Seq.iter (fun t ->
    printfn $"{t}"
    ds
    |> PSeq.map (fun d ->
      [|
        box t
        box d
        box (
          let d2 = d.AddDays 1
          ck.Query<int>
            $"select count() 
              from quote_1min_agg 
              prewhere ticker = '{t}' 
              and ts >= '{d.Year}-%02i{d.Month}-%02i{d.Day}' 
              and ts < '{d2.Year}-%02i{d2.Month}-%02i{d2.Day}'"
             |> Seq.head)
      |])
    |> i.WriteToServerAsync
    |> Async.AwaitTask
    |> Async.RunSynchronously)
  
countByDay ()
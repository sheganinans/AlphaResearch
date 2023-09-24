open ClickHouse.Client.Copy
open Dapper

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

count ()
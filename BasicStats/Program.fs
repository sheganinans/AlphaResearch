open ClickHouse.Client.Copy
open Shared.Clickhouse

let i = new ClickHouseBulkCopy (ck)
i.DestinationTableName <- "quote_1min_agg"
i.BatchSize <- 100_000

getUniqTickers ()
|> Seq.iter (fun t ->
  getLogReturns t 1 Minute
  |> Seq.map (fun r -> [| box r.ticker; box r.ts; box r.log_ret |])
  |> i.WriteToServerAsync
  |> Async.AwaitTask
  |> Async.RunSynchronously)

open System
open ClickHouse.Client.Copy
open Dapper

open FSharp.Collections.ParallelSeq
open Microsoft.ML
open Microsoft.ML.AutoML
open Microsoft.ML.Data
open Shared.Clickhouse

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
  i.DestinationTableName <- "ticker_count_by_day_normal_trading_hrs"
  i.BatchSize <- 10_000
  getUniqTickers ()
  |> Seq.iter (fun t ->
    printfn $"t: {t}"
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
              and ts < '{d2.Year}-%02i{d2.Month}-%02i{d2.Day}'
              and ((toHour(ts) >= 9 and if(toHour(ts) = 9, toMinute(ts) >= 30, true)) and toHour(ts) < 16)"
          |> Seq.head)
      |])
    |> i.WriteToServerAsync
    |> Async.AwaitTask
    |> Async.RunSynchronously)

let initTrainSplitDay = DateTime (2018, 05, 01)

let MIN_AVG_TOTAL = 340
let INTERVAL = 1
let UNIT = Minute

let importantTickers =
  let d1 = startDay
  let d2 = initTrainSplitDay
  ck.Query<string>
    $"select ticker
     from (select ticker, avg(total) as a
           from ticker_count_by_day
           where ts >= '%04i{d1.Year}-%02i{d1.Month}-%02i{d1.Day}'
             and ts < '%04i{d2.Year}-%02i{d2.Month}-%02i{d2.Day}'
             and total > 0
           group by ticker)
     where a > {MIN_AVG_TOTAL}"
  |> Array.ofSeq

printfn $"important: {importantTickers.Length}"

let nonEmptyDs =
  let d1 = startDay
  let d2 = initTrainSplitDay
  ck.Query<DateTime>
    $"select ts
     from (select ts, avg(total) as a
           from ticker_count_by_day
           where ts >= '%04i{d1.Year}-%02i{d1.Month}-%02i{d1.Day}'
             and ts < '%04i{d2.Year}-%02i{d2.Month}-%02i{d2.Day}'
           group by ts)
     where a > 0
     order by ts"
  |> Array.ofSeq

let nonEmptyDsTest =
  let d1 = initTrainSplitDay
  ck.Query<DateTime>
    $"select ts
     from (select ts, avg(total) as a
           from ticker_count_by_day
           where ts >= '%04i{d1.Year}-%02i{d1.Month}-%02i{d1.Day}'
           group by ts)
     where a > 0
     order by ts
     limit 3"
  |> Array.ofSeq

let interpolatedReturns (ticker : string) (date : DateTime) (interval : Interval) (data : LogReturn<float32> []) =
  let startTime = date.AddHours(9).AddMinutes(30)
  let endTime = date.AddHours(16)
  let intervals =
    let delta = endTime - startTime
    let total = match interval with | Minute -> delta.TotalMinutes | Hour -> delta.TotalHours
    let adder = match interval with | Minute -> startTime.AddMinutes | Hour -> startTime.AddHours
    [| 1. .. total |] |> Array.map adder
  let inline zero min =
    {
      ticker      = ticker
      ts          = min
      log_ret     = 0f
    }
  [|
    let mutable i = 0
    let mutable j = 0
    while j <> intervals.Length do
      let min = intervals[j]
      j <- j + 1
      yield
        if i = data.Length
        then zero min 
        else
          let row = data[i]
          if row.ts <> min
          then zero min
          else
            i <- i + 1
            row
  |]

let scale inputs = 1f / ((Array.max inputs - Array.min inputs) * 0.5f)

let getInterpolatedReturns (t : string) (d : DateTime) =
  getLogReturns t INTERVAL d UNIT
  |> Seq.map (fun r -> { ticker = r.ticker; ts = r.ts; log_ret = float32 r.log_ret })
  |> Array.ofSeq
  |> interpolatedReturns t d UNIT

let insertFeatures () =
  use i = new ClickHouseBulkCopy (ck)
  i.DestinationTableName <- "features"
  i.BatchSize <- 1000
  nonEmptyDs
  |> Array.iter (fun d ->
    printfn $"d: {d}"
    let m =
      importantTickers
      |> PSeq.mapi (fun i t -> i, getInterpolatedReturns t d)
      |> Map.ofSeq
    [|0 .. importantTickers.Length - 1|]
    |> Array.map (fun i -> m[i])
    |> Array.transpose
    |> Array.map (fun row -> [|
      box UNIT
      box INTERVAL
      box MIN_AVG_TOTAL
      box row[0].ts; box (row |> Array.map (fun x -> x.log_ret))
    |])
    |> i.WriteToServerAsync
    |> Async.AwaitTask
    |> Async.RunSynchronously)
  
insertFeatures ()

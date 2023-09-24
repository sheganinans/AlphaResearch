module Shared.Clickhouse

open System

open ClickHouse.Client.ADO
open Dapper

type LogReturn =
  {
    ticker      : string
    ts          : DateTime
    log_ret     : float
  }

type Interval =
  | Minute
  | Hour
  override this.ToString () =
    match this with
    | Minute -> "minute"
    | Hour -> "hour"

let host = Environment.GetEnvironmentVariable "CK_HOST"
let pw = Environment.GetEnvironmentVariable "CK_PW"
let ck = new ClickHouseConnection ($"Host={host};Password={pw}")

let getUniqTickers () =
  ck.Query<string> "select distinct ticker from thetadata_stock_trade_quotes"

let getLogReturns
  (ticker : string)
  (n : int)
  (interval : Interval) : LogReturn seq =
  ck.Query<LogReturn> $"
    with
      toStartOfInterval(time_of_trade, interval %i{n} %A{interval}) as ts_start
    , first_value(bid) as open_bid
    , last_value(bid) as close_bid
    select
      ticker
    , date_add(%A{interval}, %i{n}, ts_start) as ts
    , log(close_bid / open_bid) as log_ret
    from thetadata_stock_trade_quotes
    prewhere ticker == '{ticker}'
    group by ticker, ts_start
    order by ticker, ts_start
  "
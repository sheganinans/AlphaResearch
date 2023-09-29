module Shared.Clickhouse

open System

open ClickHouse.Client.ADO
open Dapper

type LogReturn<'t> =
  {
    ticker      : string
    ts          : DateTime
    log_ret     : 't
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
  (date : DateTime)
  (interval : Interval) : LogReturn<float> seq =
  ck.Query<LogReturn<float>> $"
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
    and ts >= '%04i{date.Year}-%02i{date.Month}-%02i{date.Day}'
    and ts < '%04i{date.Year}-%02i{date.Month}-%02i{date.Day + 1}'
    and ((toHour(time_of_trade) >= 9 and if(toHour(time_of_trade) = 9, toMinute(time_of_trade) >= 30, true))
      and toHour(time_of_trade) < 16)
    group by ticker, ts_start
    order by ticker, ts_start
  "
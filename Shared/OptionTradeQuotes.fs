module Shared.OptionTradeQuotes

open System
open Shared.ThetaData

let BUCKET = "theta-data-options-trade-quotes"

type Data = StockTradeQuotes.Data

let toReq (sec : SecurityDescrip) =
  match sec with
  | Option d ->
    let ds = $"%04i{d.Day.Year}%02i{d.Day.Month}%02i{d.Day.Day}"
    $"http://127.0.0.1:25510/hist/option/trade_quote?root={d.Root}&start_date={ds}&end_date={ds}&strike={d.Strike}&exp={d.Exp}&right={d.Right}"
  | _ -> raise (Exception "OptionTradeQUotes.toReq: this should never happen")
  
let reqAndConcat (sec : SecurityDescrip) =
  extract
    toReq
    StockTradeQuotes.rspToData
    StockTradeQuotes.concat
    sec
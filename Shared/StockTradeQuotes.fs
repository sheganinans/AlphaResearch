module Shared.StockTradeQuotes

open System
open System.IO

open K4os.Compression.LZ4
open K4os.Compression.LZ4.Streams
open Microsoft.FSharp.Collections
open ParquetSharp

open Shared.ThetaData

let BUCKET = "theta-data-trade-quotes"

type Data =
  {
    mutable TimeOfTrade : DateTime []
    mutable Sequence    : int []
    mutable Size        : uint []
    mutable Condition   : uint16 []
    mutable Price       : float []
    mutable TimeOfQuote : DateTime []
    mutable BidSize     : uint []
    mutable Bid         : float []
    mutable BidExchange : byte []
    mutable AskSize     : uint []
    mutable Ask         : float []
    mutable AskExchange : byte []
  }
  
  
let rspToData (date : DateTime) (rsp : Rsp<float [] []>) : Data =
  let l = rsp.response.Length
  let mutable ret =
    {
      TimeOfTrade = Array.create l date
      Sequence    = Array.create l 0
      Size        = Array.create l 0u
      Condition   = Array.create l 0us
      Price       = Array.create l 0.
      TimeOfQuote = Array.create l date
      BidSize     = Array.create l 0u
      Bid         = Array.create l 0.
      BidExchange = Array.create l 0uy
      AskSize     = Array.create l 0u
      Ask         = Array.create l 0.
      AskExchange = Array.create l 0uy
    }
  rsp.response |> Array.iteri (fun i el ->
    ret.TimeOfTrade[i] <- ret.TimeOfTrade[i].AddMilliseconds <| int el[0]
    ret.Sequence[i]    <-    int el[1]
    ret.Size[i]        <-   uint el[2]
    ret.Condition[i]   <- uint16 el[3]
    ret.Price[i]       <-        el[4]
    ret.TimeOfQuote[i] <- ret.TimeOfQuote[i].AddMilliseconds <| int el[5]
    ret.BidSize[i]     <-   uint el[6]
    ret.Bid[i]         <-        el[7]
    ret.BidExchange[i] <-   byte el[8]
    ret.AskSize[i]     <-   uint el[9]
    ret.Ask[i]         <-       el[10]
    ret.AskExchange[i] <-  byte el[11])
  ret
  
  
let concat a b =
  a.TimeOfTrade <- Array.append a.TimeOfTrade b.TimeOfTrade
  a.Sequence    <- Array.append a.Sequence    b.Sequence
  a.Size        <- Array.append a.Size        b.Size
  a.Condition   <- Array.append a.Condition   b.Condition
  a.Price       <- Array.append a.Price       b.Price
  a.TimeOfQuote <- Array.append a.TimeOfQuote b.TimeOfQuote
  a.BidSize     <- Array.append a.BidSize     b.BidSize
  a.Bid         <- Array.append a.Price       b.Price
  a.BidExchange <- Array.append a.BidExchange b.BidExchange
  a.AskSize     <- Array.append a.AskSize     b.AskSize
  a.Ask         <- Array.append a.Ask         b.Ask
  a.AskExchange <- Array.append a.AskExchange b.AskExchange
  

let toReq (sec : SecurityDescrip) =
  match sec with
  | Stock (root, day) ->
    let ds = $"%04i{day.Year}%02i{day.Month}%02i{day.Day}"
    $"http://127.0.0.1:25510/hist/stock/trade_quote?root={root}&start_date={ds}&end_date={ds}"
  | _ -> raise (Exception "StockTradeQuotes.toReq: this should never happen")

let reqAndConcat (root : string) (day : DateTime) = extract toReq rspToData concat (Stock (root, day))
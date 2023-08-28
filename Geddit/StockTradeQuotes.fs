module Geddit.StockTradeQuotes

open System
open System.IO

open K4os.Compression.LZ4
open K4os.Compression.LZ4.Streams
open Microsoft.FSharp.Collections
open ParquetSharp

open Geddit.ThetaData

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
  
  
let saveData (symbol : string) (date : DateTime) (data : Data) =
  let cols : Column [] =
    [|
      Column<DateTime> "TimeOfTrade"
      Column<int> "Sequence"
      Column<uint> "Size"
      Column<uint16> "TradeCondition"
      Column<float> "Price"
      Column<DateTime> "TimeOfQuote"
      Column<uint> "BidSize"
      Column<float> "Bid"
      Column<byte> "BidExchange"
      Column<uint> "AskSize"
      Column<float> "Ask"
      Column<byte> "AskExchange"
    |]
  use ms = new MemoryStream ()
  use os = new IO.ManagedOutputStream (ms)
  (
    use f = new ParquetFileWriter (os, cols)
    use rowGroup = f.AppendRowGroup ()
    use w = rowGroup.NextColumn().LogicalWriter<DateTime> () in w.WriteBatch data.TimeOfTrade
    use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.Sequence
    use w = rowGroup.NextColumn().LogicalWriter<uint>() in w.WriteBatch data.Size
    use w = rowGroup.NextColumn().LogicalWriter<uint16>() in w.WriteBatch data.Condition
    use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch data.Price
    use w = rowGroup.NextColumn().LogicalWriter<DateTime> () in w.WriteBatch data.TimeOfQuote
    use w = rowGroup.NextColumn().LogicalWriter<uint>() in w.WriteBatch data.BidSize
    use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch data.Bid
    use w = rowGroup.NextColumn().LogicalWriter<byte>() in w.WriteBatch data.BidExchange
    use w = rowGroup.NextColumn().LogicalWriter<uint>() in w.WriteBatch data.AskSize
    use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch data.Ask
    use w = rowGroup.NextColumn().LogicalWriter<byte>() in w.WriteBatch data.AskExchange
  )
  ms.Seek (0, SeekOrigin.Begin) |> ignore
  let settings = LZ4EncoderSettings ()
  settings.CompressionLevel <- LZ4Level.L03_HC
  let fileName = $"%04i{date.Year}-%02i{date.Month}-%02i{date.Day}.parquet.lz4"
  use out = LZ4Stream.Encode (File.Create fileName, settings)
  ms.CopyTo out
  out.Flush ()
  out.Close ()
  Wasabi.uploadFile fileName BUCKET $"{symbol}/{fileName}"
  File.Delete fileName

let toReq (root : string) (day : DateTime) =
  let ds = $"%04i{day.Year}%02i{day.Month}%02i{day.Day}"
  $"http://127.0.0.1:25510/hist/stock/trade_quote?root={root}&start_date={ds}&end_date={ds}"

let reqAndConcat (root : string) (day : DateTime) =
  extract
    toReq
    rspToData
    (fun a b ->
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
      a.AskExchange <- Array.append a.AskExchange b.AskExchange)
    root
    day
module Shared.FileOps

open System

open System.IO
open ParquetSharp

open Shared.ThetaData

let toFileName (descrip : SecurityDescrip) =
  match descrip with
  | SecurityDescrip.Stock (symbol, date) -> $"{symbol}/%04i{date.Year}-%02i{date.Month}-%02i{date.Day}.parquet.lz4"
  | SecurityDescrip.Option o ->
    $"data/{o.Root}/%04i{o.Day.Year}%02i{o.Day.Month}%02i{o.Day.Day}/{o.Exp}-{o.Right}-{o.Strike}.parquet.lz4"

let saveData (descrip : SecurityDescrip) (data : StockTradeQuotes.Data) =
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
  let fileName = toFileName descrip
  (
    use f = File.Create fileName
    (
      use f = new ParquetFileWriter (f, cols, compression=Compression.Lz4)
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
    f.Flush ()
    f.Close ()
  )
  Wasabi.uploadPath fileName OptionTradeQuotes.BUCKET fileName
  File.Delete fileName
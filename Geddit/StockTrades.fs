module Geddit.StockTrades

open System
open System.IO

open K4os.Compression.LZ4
open K4os.Compression.LZ4.Streams
open ParquetSharp

open Geddit.ThetaData
open Geddit.Wasabi
open Discord

let BUCKET = "theta-data-stock-trades"


type Data =
  {
    mutable time      : DateTime []
    mutable sequence  : int []
    mutable size      : int []
    mutable condition : int []
    mutable price     : float []
  }

let rspToData (date : DateTime) (rsp : Rsp<float [] []>) : Data =
  let l = rsp.response.Length
  let mutable ret =
    {
      time      = Array.create l date
      sequence  = Array.create l 0
      size      = Array.create l 0
      condition = Array.create l 0
      price     = Array.create l 0.
    }
  rsp.response |> Array.iteri (fun i el ->
    ret.time[i]      <- ret.time[i].AddMilliseconds <| int el[0]
    ret.sequence[i]  <- int el[1]
    ret.size[i]      <- int el[2]
    ret.condition[i] <- int el[3]
    ret.price[i]     <-     el[4])
  ret

let saveData (symbol : string) (date : DateTime) (data : Data) =
  let cols : Column [] =
    [|
      Column<DateTime> "time"
      Column<int> "seq_num"
      Column<int> "size"
      Column<int> "trade_condition"
      Column<float> "price"
    |]
  let fileName = $"%04i{date.Year}-%02i{date.Month}-%02i{date.Day}.parquet.lz4"
  use ms = new MemoryStream ()
  use os = new IO.ManagedOutputStream (ms)
  (
    use f = new ParquetFileWriter (os, cols)
    use rowGroup = f.AppendRowGroup ()
    use w = rowGroup.NextColumn().LogicalWriter<DateTime> () in w.WriteBatch data.time
    use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.sequence
    use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.size
    use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.condition
    use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch data.price
  )
  ms.Seek (0, SeekOrigin.Begin) |> ignore
  let settings = LZ4EncoderSettings ()
  settings.CompressionLevel <- LZ4Level.L03_HC
  use out = LZ4Stream.Encode (File.Create fileName, settings)
  ms.CopyTo out
  out.Flush ()
  out.Close ()
  uploadFile fileName BUCKET $"{symbol}/{fileName}"
  File.Delete fileName

let toReq (root : string) (day : DateTime) =
  let ds = $"%04i{day.Year}%02i{day.Month}%02i{day.Day}"
  $"http://127.0.0.1:25510/hist/stock/trade?root={root}&start_date={ds}&end_date={ds}"

let reqAndSave (root : string) (day : DateTime) =
  extract
    toReq
    rspToData
    (fun a b ->
      a.time      <- Array.append a.time      b.time
      a.sequence  <- Array.append a.sequence  b.sequence
      a.size      <- Array.append a.size      b.size
      a.condition <- Array.append a.condition b.condition
      a.price     <- Array.append a.price     b.price)
    root
    day

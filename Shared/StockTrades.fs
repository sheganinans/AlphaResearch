module Shared.StockTrades

open System
open System.IO

open ParquetSharp

open Shared.ThetaData

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
  (
    use f = File.Create fileName
    (
      use f = new ParquetFileWriter (f, cols, compression=Compression.Lz4)
      use rowGroup = f.AppendRowGroup ()
      use w = rowGroup.NextColumn().LogicalWriter<DateTime> () in w.WriteBatch data.time
      use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.sequence
      use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.size
      use w = rowGroup.NextColumn().LogicalWriter<int>() in w.WriteBatch data.condition
      use w = rowGroup.NextColumn().LogicalWriter<float>() in w.WriteBatch data.price
    )
    f.Flush ()
    f.Close ()
  )
  Wasabi.uploadPath fileName BUCKET fileName
  File.Delete fileName

let toReq (sec : SecurityDescrip) =
  match sec with
  | Stock (root, day) ->
    let ds = $"%04i{day.Year}%02i{day.Month}%02i{day.Day}"
    $"http://127.0.0.1:25510/hist/stock/trade?root={root}&start_date={ds}&end_date={ds}"
  | _ -> raise (Exception "StockTrades.toReq: this should never happen")
  
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
    (Stock (root, day))

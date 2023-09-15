open System
open System.IO

open Amazon.S3.Model
open ClickHouse.Client.ADO
open ClickHouse.Client.Copy
open FSharp.Collections.ParallelSeq
open K4os.Compression.LZ4.Streams
open ParquetSharp

open Shared
open Shared.Wasabi

let chunked () =
  let mutable currSym = ""
  let mutable acc = []
  let objToSym (o : S3Object) = o.Key.Split('/')[0]
  seq {
    for o in getWasabiObjs StockTradeQuotes.BUCKET "" do
      if currSym = "" then currSym <- objToSym o
      if currSym <> objToSym o
      then
        yield acc
        currSym <- objToSym o
        acc <- [o.Key]
      else
        acc <- o.Key :: acc
  }


let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let ds =
  seq { 0..(endDay-startDay).Days - 1 }
  |> Seq.map (startDay.AddDays << float)
  |> Set.ofSeq


using (new ClickHouseConnection ("Host=...;...")) (fun c ->
  use i = new ClickHouseBulkCopy (c)
  i.DestinationTableName <- "db.tbl"
  i.BatchSize <- 100_000
  
  let uploadFile (f : string) =
    printfn $"{f}"
    let ticker = f.Split('/')[0]
    using (LZ4Stream.Decode (File.OpenRead f)) (fun source ->
      using (new ParquetFileReader (source)) (fun file ->
        use rowGroup = file.RowGroup 0
        let ts = Array.create (int rowGroup.MetaData.NumRows) (box ticker)
        let c0 = rowGroup.Column(0).LogicalReader<DateTime>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map box
        let c2 = rowGroup.Column(2).LogicalReader<uint>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map box
        let c3 = rowGroup.Column(3).LogicalReader<uint16>().ReadAll (int rowGroup.MetaData.NumRows)
                 |> Array.map (fun e -> (e |> int |> enum<TradeCondition.TradeCondition>).ToString () |> box)
        let c4 = rowGroup.Column(4).LogicalReader<float>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map (float32 >> box)
        let c5 = rowGroup.Column(5).LogicalReader<DateTime>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map box
        let c6 = rowGroup.Column(6).LogicalReader<uint>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map box
        let c7 = rowGroup.Column(7).LogicalReader<float>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map (float32 >> box)
        let c8 = rowGroup.Column(8).LogicalReader<byte>().ReadAll (int rowGroup.MetaData.NumRows)
                  |> Array.map (fun e -> (e |> int |> enum<Exchange.Exchange>).ToString () |> box)
        let c9 = rowGroup.Column(9).LogicalReader<uint>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map box
        let c10 = rowGroup.Column(10).LogicalReader<float>().ReadAll (int rowGroup.MetaData.NumRows) |> Array.map (float32 >> box)
        let c11 = rowGroup.Column(11).LogicalReader<byte>().ReadAll (int rowGroup.MetaData.NumRows)
                  |> Array.map (fun e -> (e |> int |> enum<Exchange.Exchange>).ToString () |> box)
        [| ts; c0; c2; c3; c4; c5; c6; c7; c8; c9; c10; c11 |]
        |> Array.transpose
        |> i.WriteToServerAsync
        |> Async.AwaitTask
        |> Async.RunSynchronously))

  chunked ()
  |> Seq.iter (fun chunk ->
    let root = chunk[0].Split('/')[0]
    let map =
      chunk
      |> List.filter (fun s -> (not <| s.Contains "nodata.txt") && (not <| s.Contains ".err"))
      |> List.map (fun s ->
        try
          let d = ((s.Split('/')[1]).Split('.')[0]).Split '-' |> Array.map int
          Some <| (DateTime (d[0], d[1], d[2]), s)
        with _ -> None)
      |> List.choose id
      |> Map.ofList
    let job = ds |> Seq.map (fun d -> map |> Map.tryFind d) |> Seq.choose id
    job |> PSeq.iter (fun f -> downloadFile f StockTradeQuotes.BUCKET f)
    job |> Seq.iter uploadFile
    job |> PSeq.iter File.Delete
    Directory.Delete root))
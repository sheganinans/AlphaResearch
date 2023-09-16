open System
open System.Collections.Concurrent
open System.IO

open Amazon.S3.Model
open ClickHouse.Client.ADO
open ClickHouse.Client.Copy
open FSharp.Collections.ParallelSeq
open K4os.Compression.LZ4.Streams
open ParquetSharp

open Shared
open Shared.Discord
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

let pw = Environment.GetEnvironmentVariable "CK_PW"

type private SyncErr = class end
type private SyncUpload = class end

type Msg =
  | Clear
  | Data of string * obj [] [] option

let go () =
  let finishedSet =
    try File.ReadLines "./finished.txt" |> Set.ofSeq
    with _ -> Set.empty
  using (new ClickHouseConnection ($"Host=127.0.0.1;Password={pw}")) (fun c ->
    use errSw = new StreamWriter ("./errs.txt")
    use finishedSw = new StreamWriter ("./finished.txt")
    c.Open ()

    use i = new ClickHouseBulkCopy (c)
    i.DestinationTableName <- "thetadata_stock_trade_quotes"
    i.BatchSize <- 100_000
    let s = ConcurrentDictionary<string, unit> ()

    let setupFile (f : string) =
      try
        s.TryAdd (f, ()) |> ignore
        downloadFile f StockTradeQuotes.BUCKET f
        let ticker = f.Split('/')[0]
        use source = LZ4Stream.Decode (File.OpenRead f)
        use ms = new MemoryStream ()
        source.CopyTo ms
        source.Close ()
        source.Dispose ()
        File.Delete f
        use file = new ParquetFileReader (ms)
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
        file.Close ()
        file.Dispose ()
        [| ts; c0; c2; c3; c4; c5; c6; c7; c8; c9; c10; c11 |]
        |> Array.transpose
        |> Some
      with err ->
        lock typeof<SyncErr> (fun () -> errSw.WriteLine f; errSw.Flush ())
        printfn $"{err}"
        None

    let uploadMailbox = MailboxProcessor.Start (fun inbox ->
      async {
        let mutable acc = [||]
        let write () =
          acc
          |> i.WriteToServerAsync
          |> Async.AwaitTask
          |> Async.RunSynchronously
          acc <- [||]

        while true do
          let! (msg : Msg) = inbox.Receive ()
          match msg with
          | Clear -> write ()
          | Data (f, msg) ->
            match msg with
            | None -> ()
            | Some msg ->
              acc <- Array.append acc msg
              if acc.Length > 50_000 then write ()
            s.TryRemove f |> ignore
      })

    chunked ()
    |> Seq.iter (fun chunk ->
      let root = chunk[0].Split('/')[0]
      if not <| finishedSet.Contains root
      then
        printfn $"{root}"
        discord.SendNotification $"{root}" |> Async.Start
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
        if job |> Seq.length > 0
        then
          job |> PSeq.iter (fun f ->
            while s.Count > 24 do Async.Sleep 100 |> Async.RunSynchronously
            uploadMailbox.Post (Data (f, setupFile f)))
          while s.Count <> 0 do Async.Sleep 100 |> Async.RunSynchronously
          uploadMailbox.Post Clear
          finishedSw.WriteLine root
          finishedSw.Flush ()
          Directory.Delete (root, true)))
  
let mkSql (agg : int) =
  let mkBlock (e : string) (n: string) =
    $"""
, sum({e}) as sum_{n}
, avg({e}) as avg_{n}
, max({e}) as max_{n}
, stddevPopStable({e}) as std_dev_{n}
, skewPop({e}) as skew_{n}
, kurtPop({e}) as kurt_{n}
, quantiles(0.013,0.023,0.159,0.5,0.841,0.977,0.998)(size) as quantiles_{n}
, entropy({e}) as entropy_{n}"""
  
  $"""
with 
  toStartOfInterval(time_of_trade, interval {agg} minute) as ts
, if(neighbor(time_of_trade, -1) = '1969-12-31 19:00:00', ts, neighbor(time_of_trade, -1)) as prev
, if(prev == time_of_trade, 1, date_diff('s', prev, time_of_trade)) as time_diff
select 
  ticker
, ts
, count() as count

, first_value(price) as open
, last_value(price) as close
{mkBlock "size" "size"}
{mkBlock "bid_size" "bid_size"}
{mkBlock "ask_size" "ask_size"}
{mkBlock "price" "price"}
{mkBlock "ask - bid" "spread"}
{mkBlock "ask_size - bid_size" "size_diff"}

, covarPopStable(price, bid) as covar_price_bid
, covarPopStable(price, ask) as covar_price_ask
, covarPopStable(bid, ask) as covar_bid_ask

, sum(price * size) as liquidity
, liquidity / sum_size as vwap
, close - open as diff
, log(close/open) as log_ret

, sum(time_diff) as twap_denom
, sum(price * time_diff) as twap_nom
, twap_nom / twap_denom as twap

, sumMap(map(condition, 1)) as condition_map
, sumMap(map(bid_exchange, 1)) as bid_exchange_map
, sumMap(map(ask_exchange, 1)) as ask_exchange_map

from thetadata_stock_trade_quotes
group by ticker, ts
order by ticker, ts
limit 20;"""
  
//printfn $"{mkSql 15}"
go ()
open System

open ParquetSharp
open Plotly.NET

open Shared.Exchange
open Shared.TradeCondition

using (new ParquetFileReader "AAPL-2020-07-31.parquet") (fun file ->
  use rowGroup = file.RowGroup 0
  let s = 100_000 //1_000_000 //2500
  let t = 100_000
  let n = int rowGroup.MetaData.NumRows
  printfn $"{n}"
  let timeOfTrade = rowGroup.Column(0).LogicalReader<DateTime>().ReadAll n |> Array.skip s |> Array.take t
  let tradeSize = rowGroup.Column(2).LogicalReader<uint>().ReadAll n |> Array.skip s |> Array.take t
  let tradeCond = rowGroup.Column(3).LogicalReader<uint16>().ReadAll n |> Array.skip s |> Array.take t
  let price = rowGroup.Column(4).LogicalReader<float>().ReadAll n |> Array.skip s |> Array.take t
  let bid = rowGroup.Column(7).LogicalReader<float>().ReadAll n |> Array.skip s |> Array.take t
  let ask = rowGroup.Column(10).LogicalReader<float>().ReadAll n |> Array.skip s |> Array.take t
  let exch = rowGroup.Column(8).LogicalReader<byte>().ReadAll n |> Array.skip s |> Array.take t
  let uniqConds = tradeCond |> Set.ofArray |> Set.toArray
  let uniqExch = exch |> Set.ofArray |> Set.toArray
  let w = 200
  let tradeConScan =
    tradeCond
    |> Array.windowed w
    |> Array.map (fun chunk ->
      let counts =
        chunk |> Array.fold
           (fun acc x -> acc |> Map.change x (function | None -> Some 1 | Some n -> Some (n + 1)))
           Map.empty
      uniqConds |> Array.map (fun c -> float (counts |> Map.tryFind c |> Option.defaultValue 0) / float w))
    |> Array.transpose
    |> Array.zip uniqConds
    |> Array.map (fun (cond, col) ->
      Chart.Line (timeOfTrade |> Array.skip (timeOfTrade.Length - col.Length), col, Name= $"{enum<TradeCondition> (int cond)}"))
    |> Chart.combine
  let exchConScan =
    exch
    |> Array.windowed w
    |> Array.map (fun chunk ->
      let counts =
        chunk |> Array.fold
           (fun acc x -> acc |> Map.change x (function | None -> Some 1 | Some n -> Some (n + 1)))
           Map.empty
      uniqExch |> Array.map (fun c -> float (counts |> Map.tryFind c |> Option.defaultValue 0) / float w))
    |> Array.transpose
    |> Array.zip uniqExch
    |> Array.map (fun (exch, col) ->
      Chart.Line (timeOfTrade |> Array.skip (timeOfTrade.Length - col.Length), col, Name= $"{enum<Exchange> (int exch)}"))
    |> Chart.combine

  [
    [
      Chart.Line (timeOfTrade, bid,  Name="bid")
      Chart.Line (timeOfTrade, ask,  Name="ask")
      Chart.Line (timeOfTrade, price,  Name="price")
    ] |> Chart.combine
    tradeConScan
    exchConScan
    Chart.Line (timeOfTrade, tradeSize |> Array.map (fun v -> log (float (v + 1u))), Name="log(size)")
  ]
  |> Chart.SingleStack (Pattern = StyleParam.LayoutGridPattern.Coupled)
  |> Chart.withLayout (Layout.init (Width = 1600, Height = 900))
  |> Chart.show)
open System
open System.IO

open System.Threading
open FSharp.Collections.ParallelSeq

open Shared
open Shared.ThetaData
open Shared.Discord

let thetaData = Theta ()

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let TOTAL_DAYS = (endDay-startDay).Days


let finishedSet =
  try File.ReadLines "finished.txt" |> Set.ofSeq
  with _ -> Set.empty

let roots =
  Async.Sleep 7000 |> Async.RunSynchronously
  printfn "get roots"
  match Roots.getStockRoots () with
  | Error _ -> [||]
  | Result.Ok roots ->
    roots
    |> Array.map (fun root -> root.Replace ('/', '.'))
    |> Array.filter (not << finishedSet.Contains)

type private SyncCount = class end
type private SyncNoData = class end

type DataType = NoData | Data

let rec finished (root : string) = 
  async {
    try
      nextSymbol () |> Async.Start
      while (Directory.GetFiles root).Length <> 0 do Async.Sleep 50 |> Async.RunSynchronously 
      printfn $"finished: {root}"
      async {
        do! Async.Sleep 3000
        let noDataFile = $"{root}.nodata.txt"
        Wasabi.uploadPath noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
        printfn "uploaded nodata file"
        File.Delete noDataFile            
        Directory.Delete root
        using (File.AppendText "finished.txt") (fun sw -> sw.WriteLine root)
      } |> Async.Start
    with err -> discord.SendAlert $"finishedMailbox: {err}" |> Async.Start
  }

and counter ((root, day, data) : string * DateTime * DataType) =
  let mutable m = Map.empty
  async {
    try
      let f = $"{root}/%04i{day.Year}-%02i{day.Month}-%02i{day.Day}.parquet.lz4"
      async {
        match data with
        | NoData ->
          let noDataFile = $"{root}.nodata.txt"
          lock typeof<SyncNoData> (fun () ->
            using (File.AppendText noDataFile) (fun sw ->
              sw.WriteLine (day.ToString ())
              sw.Flush ()))
        | Data ->
          Wasabi.uploadPath f StockTradeQuotes.BUCKET f
          File.Delete f
      } |> Async.Start
      let c =
        lock typeof<SyncCount> (fun () ->
          m <- m |> Map.change root (function | None -> Some 1 | Some n -> Some (n + 1))
          m |> Map.find root)
      if TOTAL_DAYS = c then finished root |> Async.Start
    with err -> discord.SendAlert $"counterMailbox: {err}" |> Async.Start
  }

and getData (root : string) =
  async {
    try
      discord.SendNotification $"starting: {root}" |> Async.Start
      let mutable trySet =
        seq { 0..(endDay-startDay).Days - 1 }
        |> Seq.map (startDay.AddDays << float)
        |> Set.ofSeq
        
      while trySet.Count <> 0 do
        trySet <- 
          trySet
          |> Seq.chunkBySize (match (trySet.Count / Environment.ProcessorCount) / 16 with 0 -> 1 | c -> c)
          |> PSeq.withDegreeOfParallelism Environment.ProcessorCount
          |> (Set.empty |> PSeq.fold (fun retries chunk ->
              retries |> Set.union
                (chunk
                |> PSeq.withDegreeOfParallelism 2
                |> (Set.empty |> PSeq.fold (fun retries day ->
                    match StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously with
                    | RspStatus.Err err ->
                      discord.SendAlert $"getDataMailbox1: {err}" |> Async.Start
                      retries.Add day
                    | RspStatus.Disconnected ->
                      thetaData.Reset ()
                      retries.Add day
                    | RspStatus.NoData ->
                      counter (root, day, NoData) |> Async.Start
                      retries
                    | RspStatus.Ok data ->
                      try
                        StockTradeQuotes.saveData root day data
                        counter (root, day, Data) |> Async.Start
                        retries
                      with err ->
                        discord.SendAlert $"getDataMailbox2: {err}" |> Async.Start                
                        retries.Add day)))))
        if trySet.Count <> 0
        then
          do! Async.Sleep 20_000
          do! discord.SendAlert $"restarting {root} with {trySet.Count} saved dates"
    with err -> discord.SendAlert $"getDataMailbox3: {err}" |> Async.Start
  }
  
and nextSymbol () =
  let mutable i = 0
  async {
    try
      let r = roots[i]
      printfn $"symbol: {r}"
      getData r |> Async.Start
      i <- i + 1
    with err -> discord.SendAlert $"symbolMailbox: {err}" |> Async.Start
  }

Thread.Sleep 1000
nextSymbol () |> Async.Start
Thread.Sleep -1
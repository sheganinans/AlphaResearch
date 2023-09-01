open System
open System.IO

open System.Threading
open FSharp.Collections.ParallelSeq
open Geddit
open Geddit.ThetaData
open Geddit.Discord

let thetaData = Theta ()

let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let TOTAL_DAYS = (endDay-startDay).Days


let finished =
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
    |> Array.filter (not << finished.Contains)

type private SyncFinish = class end
type private SyncCount = class end
type private SyncNoData = class end
type private SyncGo = class end

let mutable nextSymbol = fun () -> ()

type DataType = NoData | Data

let rec finishedMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    while true do
      let! (root : string) = inbox.Receive ()
      lock typeof<SyncFinish> (fun () ->
        try
          nextSymbol ()
          while (Directory.GetFiles root).Length <> 0 do Async.Sleep 50 |> Async.RunSynchronously 
          printfn $"finished: {root}"
          async {
            do! Async.Sleep 3000
            let noDataFile = $"{root}.nodata.txt"
            Wasabi.uploadFile noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
            printfn "uploaded nodata file"
            File.Delete noDataFile            
            Directory.Delete root
            using (File.AppendText "finished.txt") (fun sw -> sw.WriteLine root)
          } |> Async.Start
        with err -> discord.SendAlert $"finishedMailbox: {err}" |> Async.Start)
  })

and counterMailbox = MailboxProcessor.Start (fun inbox ->
  let mutable m = Map.empty
  async {
    try
      while true do
        let! ((root, day, data) : string * DateTime * DataType) = inbox.Receive ()
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
            Wasabi.uploadFile f StockTradeQuotes.BUCKET f
            File.Delete f
        } |> Async.Start
        let c =
          lock typeof<SyncCount> (fun () ->
            m <- m |> Map.change root (function | None -> Some 1 | Some n -> Some (n + 1))
            m |> Map.find root)
        if TOTAL_DAYS = c then finishedMailbox.Post root
    with err -> discord.SendAlert $"counterMailbox: {err}" |> Async.Start
  })

and getDataMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    while true do
      try
        let! (root : string) = inbox.Receive ()
        discord.SendNotification $"starting: {root}" |> Async.Start
        let mutable trySet =
          seq { 0..(endDay-startDay).Days - 1 }
          |> Seq.map (startDay.AddDays << float)
          |> Set.ofSeq
          
        let mutable noDataAcc = []
        let mutable disconns = []
        let mutable errors = []
          
        while trySet.Count <> 0 do
          trySet
          |> Seq.chunkBySize 16
          |> PSeq.withDegreeOfParallelism 4
          |> PSeq.iter (fun chunk ->
            chunk
            |> PSeq.withDegreeOfParallelism 2
            |> PSeq.iter (fun day ->
              match StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously with
              | RspStatus.Err err -> lock typeof<SyncGo> (fun () ->
                discord.SendAlert $"getDataMailbox1: {err}" |> Async.Start
                errors <- day :: errors)
              | RspStatus.Disconnected -> lock typeof<SyncGo> (fun () ->
                thetaData.Reset ()
                disconns <- day:: disconns)
              | RspStatus.NoData -> lock typeof<SyncGo> (fun () ->
                counterMailbox.Post (root, day, NoData)
                noDataAcc <- day :: noDataAcc)
              | RspStatus.Ok data -> lock typeof<SyncGo> (fun () ->
                try
                  StockTradeQuotes.saveData root day data
                  counterMailbox.Post (root, day, Data)
                with err ->
                  discord.SendAlert $"getDataMailbox2: {err}" |> Async.Start                
                  disconns <- day :: disconns)))
          trySet <- disconns |> Set.ofList |> Set.union (errors |> Set.ofList)
          disconns <- []
          errors <- []
          if trySet.Count <> 0
          then
            do! Async.Sleep 20_000
            do! discord.SendAlert $"restarting {root} with {trySet.Count} saved dates"
      with err -> discord.SendAlert $"getDataMailbox3: {err}" |> Async.Start
  })
  
and symbolMailbox = MailboxProcessor.Start (fun inbox ->
  let mutable i = 0
  async {
    try
      while true do
        let! () = inbox.Receive ()
        let r = roots[i]
        printfn $"symbol: {r}"
        getDataMailbox.Post r
        i <- i + 1
    with err -> discord.SendAlert $"symbolMailbox: {err}" |> Async.Start
  })

nextSymbol <- symbolMailbox.Post

Thread.Sleep 1000
symbolMailbox.Post ()
Thread.Sleep -1
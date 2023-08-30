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
  | Error err -> [||]
  | Result.Ok roots ->
    roots
    |> Array.map (fun root -> root.Replace ('/', '.'))
    |> Array.filter (not << finished.Contains)

type private SyncFinish = class end
type private SyncCount = class end
type private SyncGo = class end

let mutable nextSymbol = fun () -> ()

type DataType = NoData | Data

let rec finishedMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    while true do
      let! (root : string) = inbox.Receive ()
      lock typeof<SyncFinish> (fun () ->
        try
          printfn $"finished: {root}"
          let noDataFile = $"{root}.nodata.txt"
          Wasabi.uploadFile noDataFile StockTradeQuotes.BUCKET $"{root}/nodata.txt"
          printfn "uploaded no data file"
          File.Delete noDataFile
          File.Delete $"{root}.dates.txt"
          while (Directory.GetFiles root).Length <> 0 do Async.Sleep 100 |> Async.RunSynchronously 
          Directory.Delete root

          use sw = File.AppendText "finished.txt"
          sw.WriteLine root
          nextSymbol ()
        with err -> discord.SendAlert $"finishedMailbox: {err}" |> Async.Start)
  })

and counterMailbox = MailboxProcessor.Start (fun inbox ->
  let mutable m = Map.empty
  let mutable now = DateTime.Now
  let mutable ms = 0.
  async {
    try
      while true do
        let! ((root, day, data) : string * DateTime * DataType) = inbox.Receive ()
        let f = $"{root}/%04i{day.Year}-%02i{day.Month}-%02i{day.Day}.parquet.lz4"
        async {
          match data with
          | NoData ->
              lock typeof<SyncCount> (fun () ->
                let noDataFile = $"{root}.nodata.txt"
                use sw = File.AppendText noDataFile
                sw.WriteLine (day.ToString ())
                sw.Flush ()
                sw.Close ())            
          | Data ->
              Wasabi.uploadFile f StockTradeQuotes.BUCKET f
              File.Delete f
              use sw = File.AppendText $"{root}.dates.txt"
              sw.WriteLine (day.ToString ())
              sw.Flush ()
        } |> Async.Start
        let c =
          lock typeof<SyncCount> (fun () ->
            m <- m |> Map.change root (function | None -> Some 1 | Some n -> Some (n + 1))
            ms <- float (DateTime.Now - now).Milliseconds
            now <- DateTime.Now
            m |> Map.find root)
        printfn $"%02.2f{100. * (float c / float TOTAL_DAYS)}%% {f} %04.2f{ms}ms"
        if TOTAL_DAYS = c then finishedMailbox.Post root
    with err -> discord.SendAlert $"counterMailbox: {err}" |> Async.Start
  })

and getDataMailbox = MailboxProcessor.Start (fun inbox ->
  async {
    try
      while true do
        let! (root : string) = inbox.Receive ()
        Directory.CreateDirectory root |> ignore
        discord.SendNotification $"starting: {root}" |> Async.Start
        let skipDates =
          try File.ReadLines $"{root}.dates.txt" |> Seq.map DateTime.Parse |> Set.ofSeq
          with _ -> Set.empty
        skipDates |> Set.iter (fun d -> counterMailbox.Post (root, d, NoData))
        let mutable trySet =
          seq { 0..(endDay-startDay).Days - 1 }
          |> Seq.map (startDay.AddDays << float)
          |> Seq.filter (not << skipDates.Contains)
          |> Set.ofSeq
          
        let mutable noDataAcc = []
        let mutable disconns = []
        let mutable errors = []
          
        while trySet.Count <> 0 do
          trySet
          |> PSeq.withDegreeOfParallelism 6
          |> PSeq.iter (fun day ->
            match StockTradeQuotes.reqAndConcat root day |> Async.RunSynchronously with
            | RspStatus.Err err -> lock typeof<SyncGo> (fun () -> printfn $"{err}"; errors <- day :: errors)
            | RspStatus.Disconnected -> lock typeof<SyncGo> (fun () ->
              printfn "disconnected!"
              thetaData.Reset ()
              disconns <- day:: disconns)
            | RspStatus.NoData -> lock typeof<SyncGo> (fun () ->
              counterMailbox.Post (root, day, NoData)
              noDataAcc <- day :: noDataAcc)
            | RspStatus.Ok data -> lock typeof<SyncGo> (fun () ->
              StockTradeQuotes.saveData root day data
              counterMailbox.Post (root, day, Data)))
          
          trySet <- disconns |> Set.ofList |> Set.union (errors |> Set.ofList)
    with err -> discord.SendAlert $"getDataMailbox: {err}" |> Async.Start
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
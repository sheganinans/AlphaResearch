  open System
  open System.Collections.Concurrent
  open System.IO

  open Amazon.S3.Model

  open FSharp.Collections.ParallelSeq
  open Shared
  open Shared.Discord
  open Shared.ThetaData

  let BUCKET = StockTradeQuotes.BUCKET

  let chunked () =
    let mutable currSym = ""
    let mutable acc = []
    let objToSym (o : S3Object) = o.Key.Split('/')[0]
    seq {
      for o in Wasabi.getWasabiObjs BUCKET "" do
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

  let mutable good = 0
  let mutable bad = 0

  let HTML_CONCURRENCY = 40

  let thetaData = Theta ()

  type private SyncNoData = class end

  let s = ConcurrentDictionary<string * DateTime, unit> ()

  let ds =
    seq { 0..(endDay-startDay).Days - 1 }
    |> Seq.map (startDay.AddDays << float)
    |> Set.ofSeq

  chunked ()
  |> PSeq.iter (fun job ->
    let root = job[0].Split('/')[0]
    let noData = 
      match job |> List.tryFind (fun s -> s.Contains "nodata.txt") with
      | None -> Set.empty
      | Some f ->
        Wasabi.downloadFile f BUCKET f
        let ret =
          File.ReadLines f
          |> Seq.map (fun s -> try Some <| DateTime.Parse s with _ -> None)
          |> Seq.choose id
          |> Set.ofSeq
          |> Set.intersect ds
        File.Delete f
        ret
    let job =
      job
      |> List.filter (fun s -> (not <| s.Contains "nodata.txt") && (not <| s.Contains ".err"))
      |> List.map (fun s ->
        try
          let d = ((s.Split('/')[1]).Split('.')[0]).Split '-' |> Array.map int
          Some <| DateTime (d[0], d[1], d[2])
        with _ -> None)
      |> List.choose id
      |> Set.ofList
      |> Set.intersect ds
    let noData = noData - job
    let c = job.Count + noData.Count
    if c = ds.Count
    then good <- good + 1
    else
      printfn $"bad {root}: {c}"
      bad <- bad + 1
      if c > ds.Count
      then
        printfn $"{root}. js: {job.Count}. nd:{noData.Count}. +:{c}."
      else
        Directory.CreateDirectory root |> ignore
        let noDataFile = $"{root}/nodata.txt"
        use sw = new StreamWriter (noDataFile)
        noData |> Set.iter (fun d -> sw.WriteLine (d.ToString ()))
        ds
        |> Seq.filter (fun day -> (not <| noData.Contains day) && (not <| job.Contains day))
        |> Seq.iter (fun day ->
          s.TryAdd ((root, day), ()) |> ignore
          printfn $"starting: {root} {day}"
          while s.Count > HTML_CONCURRENCY do Async.Sleep 10 |> Async.RunSynchronously
          async {
            let mutable retry = true
            let inline finishedSuccessfully () =
              printfn $"finished: {root} {day}"
              s.TryRemove ((root, day)) |> ignore
              retry <- false
            while retry do
              match! StockTradeQuotes.reqAndConcat root day with
              | RspStatus.Err err -> discord.SendAlert $"repair1: {err}" |> Async.Start
              | RspStatus.Disconnected ->
                thetaData.Reset ()
                do! Async.Sleep 10_000
              | RspStatus.NoData ->
                printfn "no data"
                lock typeof<SyncNoData> (fun () -> sw.WriteLine (day.ToString ()))
                finishedSuccessfully ()
              | RspStatus.Ok data ->
                printfn $"ok: {root}"
                FileOps.saveData (SecurityDescrip.Stock (root, day)) StockTradeQuotes.BUCKET data
                finishedSuccessfully ()
            } |> Async.Start)
        let mutable finished = false
        async {
          while s.Keys |> Seq.filter (fun (r,_) -> r = root) |> Seq.length <> 0 do do! Async.Sleep 1000
          sw.Flush ()
          sw.Close ()
          Wasabi.uploadPath noDataFile StockTradeQuotes.BUCKET noDataFile
          File.Delete noDataFile
          printfn $"{root} fixed."
          finished <- true
        } |> Async.Start
        while not finished do Async.Sleep 1000 |> Async.RunSynchronously
        //printfn $"perc good: %0.2f{100. * (float good / float (good + bad))}"
        Directory.Delete root)
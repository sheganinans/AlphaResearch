open System
open System.IO
open System.Net.Http

open System.Threading
open System.Threading.Tasks
open Amazon.S3
open Amazon.S3.Model
open Amazon.S3.Transfer
open Discord
open Discord.WebSocket
open FSharp.Collections.ParallelSeq
open FSharp.Json
open K4os.Compression.LZ4
open K4os.Compression.LZ4.Streams
open ParquetSharp

let private config = AmazonS3Config  ()
config.ServiceURL <- "https://s3.wasabisys.com"
let key = Environment.GetEnvironmentVariable "WASABI_KEY"
let secret = Environment.GetEnvironmentVariable "WASABI_SECRET"
let private s3 = new AmazonS3Client (key, secret, config)

let BUCKET = "theta-data"

let uploadFile (file : string) (key : string) =
  use u = new TransferUtility (s3)
  u.Upload (file, BUCKET, key)
  u.Dispose ()

let downloadFile (file : string) (key : string) =
  use u = new TransferUtility (s3)
  u.Download (file, BUCKET, key)
  u.Dispose ()

type Header =
  {
    id         : int
    latency_ms : int
    error_type : string
    error_msg  : string
    next_page  : string
    format     : string [] option
  }

type Rsp<'t> =
  {
    header   : Header
    response : 't
  }

let getWasabiObjs () =
  let req = ListObjectsRequest ()
  req.BucketName <- BUCKET
  let mutable rsp = s3.ListObjectsAsync req |> Async.AwaitTask |> Async.RunSynchronously
  seq {
    yield! rsp.S3Objects
    while rsp.IsTruncated do
      req.Marker <- rsp.NextMarker
      rsp <- s3.ListObjectsAsync req |> Async.AwaitTask |> Async.RunSynchronously
      yield! rsp.S3Objects
  }
  
let chunked () =
  let mutable currSym = ""
  let mutable acc = []
  let objToSym (o : S3Object) = o.Key.Split('/')[0]
  seq {
    for o in getWasabiObjs () do
      if currSym = "" then currSym <- objToSym o
      if currSym <> objToSym o
      then
        yield acc
        currSym <- objToSym o
        acc <- [o.Key]
      else
        acc <- o.Key :: acc
  }
 
type ErrDescip =
  {
    ErrType    : string
    ErrDescrip : string
  }
 
type ReqRes =
  | Ok of Rsp<float [] []>
  | NoData
  | Err of ErrDescip
 
let reqUrl (url : string) =
  task {
    use client = new HttpClient ()
    client.DefaultRequestHeaders.Add ("Accept", "application/json")
    let! response = client.GetAsync url
    let! c = response.Content.ReadAsStringAsync ()
    try return Ok <| Json.deserialize<Rsp<float [] []>> c
    with _ ->
      let err = (Json.deserialize<Rsp<int []>> c).header
      match err.error_type with
      | "NO_DATA" -> return NoData
      | _ -> return Err { ErrType = err.error_type; ErrDescrip = err.error_msg }
  }
 
let reqStockHist (root : string) (date : DateTime) : Async<ReqRes> =
  task {
    let ds = $"%04i{date.Year}%02i{date.Month}%02i{date.Day}"
    printfn $"req: {root} {date}"
    return! reqUrl $"http://127.0.0.1:25510/hist/stock/trade?root={root}&start_date={ds}&end_date={ds}"
  }
  |> Async.AwaitTask
      
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
  uploadFile fileName $"{symbol}/{fileName}"
  File.Delete fileName
let startDay = DateTime (2018, 01, 01)
let endDay = DateTime (2023, 08, 01)

let mutable good = 0
let mutable bad = 0

chunked ()
|> Seq.iter (fun job ->
  let root = job[0].Split('/')[0]
  let lockObj = Object ()
  let mutable noData = 
    match job |> List.tryFind (fun s -> s.Contains "nodata.txt") with
    | None -> Set.empty
    | Some f ->
      downloadFile f f
      File.ReadLines f
      |> Seq.map (fun s -> try Some <| DateTime.Parse s with _ -> None)
      |> Seq.choose id
      |> Set.ofSeq 
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
  if job.Count + noData.Count = 2039
  then
    good <- good + 1
    printfn $"{root}: all good."
  else
    printfn $"{root}: requires fix"
    bad <- bad + 1
    seq { 0..(endDay-startDay).Days }
    |> Seq.map (startDay.AddDays << float)
    |> Seq.filter (fun day -> (not <| noData.Contains day) && (not <| job.Contains day))
    |> Seq.iter (fun day ->
      ()
      //printfn $"{root}: {day}"
      // try
      //   let rsp = reqStockHist root day |> Async.RunSynchronously
      //   match rsp with
      //   | Err err -> discord.SendAlert $"ERR1:\n{err}" |> Async.Start
      //   | NoData -> lock lockObj (fun () -> noData <- noData.Add day)
      //   | Ok rsp ->
      //     let mutable data = rspToData day rsp
      //     let mutable nextPage = rsp.header.next_page
      //     while not <| isNull nextPage && nextPage <> "null" do
      //       printfn $"{nextPage}"
      //       let rsp = reqUrl nextPage |> Async.AwaitTask |> Async.RunSynchronously
      //       match rsp with
      //       | Err err ->
      //         discord.SendAlert $"ERR2:\m{err}" |> Async.Start
      //         let fileName = $"%04i{day.Year}-%02i{day.Month}-%02i{day.Day}.err"
      //         File.AppendAllText (fileName, Json.serialize err)
      //         uploadFile fileName $"{root}/{fileName}"
      //         File.Delete fileName
      //       | NoData -> nextPage <- null
      //       | Ok rsp ->
      //         nextPage <- rsp.header.next_page
      //         let addtlData = rspToData day rsp
      //         printfn $"{data.time[0]}"
      //         data.time      <- Array.append data.time      addtlData.time
      //         data.sequence  <- Array.append data.sequence  addtlData.sequence
      //         data.size      <- Array.append data.size      addtlData.size
      //         data.condition <- Array.append data.condition addtlData.condition
      //         data.price     <- Array.append data.price     addtlData.price
      //         printfn $"{data.time |> Array.last}"
      //       saveData root day data
      //   with
      //     | :? TaskCanceledException -> () //killerMailbox.Post ()
      //     | err -> ()) //discord.SendAlert $"ERR3: {err}" |> Async.Start)
      //
    )
  printfn $"perc good: {100. * (float good / float (good + bad))}")
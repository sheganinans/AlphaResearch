module Geddit.ThetaData

open System
open System.Diagnostics
open System.Net.Http

open Discord
open SpanJson

type Header () = 
    member val id : int = 0 with get,set
    member val latency_ms : int = 0 with get, set
    member val error_type : string = null with get, set
    member val error_msg  : string = null with get, set
    member val next_page  : string = null with get, set
    member val format     : string [] = [||] with get, set

type Rsp<'t> () =
    member val header   : Header = Header () with get, set
    member val response : 't = Unchecked.defaultof<'t> with get, set

type ErrDescip =
  {
    ErrType    : string
    ErrDescrip : string
  }

type RspStatus<'t> =
  | Ok of 't
  | NoData
  | Disconnected
  | Err of ErrDescip

let reqThetaData<'t> (url : string) =
  task {
    try
      use client = new HttpClient ()
      client.DefaultRequestHeaders.Add ("Accept", "application/json")
      let! response = client.GetAsync url
      let! c = response.Content.ReadAsStringAsync ()
      try return Ok <| JsonSerializer.Generic.Utf16.Deserialize<Rsp<'t>> c
      with _ ->
        let err = (JsonSerializer.Generic.Utf16.Deserialize<Rsp<int []>> c).header
        match err.error_type with
        | "NO_DATA" -> return NoData
        | "DISCONNECTED" -> return Disconnected
        | _ -> return Err { ErrType = err.error_type; ErrDescrip = err.error_msg }
    with _ -> return Disconnected
  }
  
let inline extract<'a, 'b>
    (f : string -> DateTime -> string)
    (g : DateTime -> Rsp<'a> -> 'b)
    (h : 'b -> 'b -> unit)
    (root : string)
    (day : DateTime) : 'b RspStatus Async =
  async {
    match! f root day |> reqThetaData<'a> |> Async.AwaitTask with
    | RspStatus.NoData -> return RspStatus.NoData
    | RspStatus.Disconnected -> return RspStatus.Disconnected
    | RspStatus.Err e -> return RspStatus.Err e
    | RspStatus.Ok rsp ->
      let mutable disconn = false
      let mutable retErr = None
      let mutable data = g day rsp
      let mutable nextPage = rsp.header.next_page
      while
          not <| isNull nextPage &&
          nextPage <> "null" &&
          not retErr.IsSome &&
          not disconn do
        let! rsp = reqThetaData<'a> nextPage |> Async.AwaitTask
        match rsp with
        | Disconnected -> disconn <- true
        | Err err -> retErr <- Some err
        | NoData -> nextPage <- null
        | Ok rsp ->
          nextPage <- rsp.header.next_page
          g day rsp |> h data
      match disconn, retErr with
      | true, _ -> return RspStatus.Disconnected
      | _, Some err -> return RspStatus.Err err
      | _, _ -> return RspStatus.Ok data
  }

type private ThetaProc () =
  let theta = new Process ()
  do
    discord.SendAlert "starting new theta terminal" |> Async.Start
    theta.StartInfo.WorkingDirectory <- Environment.GetEnvironmentVariable "HOME"
    theta.StartInfo.FileName <- "java"
    theta.StartInfo.Arguments <- "-Xmx4096m -jar ThetaTerminal.jar creds=creds"
    theta.Start () |> ignore
    
  member this.Proc = theta

type private SyncTheta = class end

type Theta () =
  let mutable thetaProc = ThetaProc ()

  let resetTheta = MailboxProcessor.Start (fun inbox ->
    let mutable lastTime = DateTime.Now
    async {
      while true do
        let! () = inbox.Receive ()

        lock typeof<SyncTheta> (fun () ->
          if (DateTime.Now - lastTime).Seconds > 10
          then
            printfn "killing thetadata."
            discord.SendAlert "killing thetadata." |> Async.Start
            let td = thetaProc.Proc
            td.Kill ()
            td.Dispose ()
            thetaProc <- ThetaProc ()

          lastTime <- DateTime.Now)
    })
  
  member this.Reset () = resetTheta.Post ()

  member this.Kill () = thetaProc.Proc.Kill ()
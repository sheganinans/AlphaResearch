module Geddit.ThetaData

open System
open System.Diagnostics
open System.Net.Http

open Discord
open FSharp.Json

type Header =
  {
    id         : int
    latency_ms : int
    error_type : string option
    error_msg  : string option
    next_page  : string option
    format     : string [] option
  }

type Rsp<'t> =
  {
    header   : Header
    response : 't
  }

type ErrDescip =
  {
    ErrType    : string option
    ErrDescrip : string option
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
      try return Ok <| Json.deserialize<Rsp<'t>> c
      with _ ->
        let err = (Json.deserialize<Rsp<int []>> c).header
        match err.error_type with
        | Some "NO_DATA" -> return NoData
        | Some "DISCONNECTED" -> return Disconnected
        | _ -> return Err { ErrType = err.error_type; ErrDescrip = err.error_msg }
    with err ->
      discord.SendAlert $"{err}" |> Async.Start
      return Disconnected
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
          not <| nextPage.IsNone &&
          nextPage <> Some "null" &&
          not retErr.IsSome &&
          not disconn do
        let! rsp = reqThetaData<'a> nextPage.Value |> Async.AwaitTask
        match rsp with
        | Disconnected -> disconn <- true
        | Err err -> retErr <- Some err
        | NoData -> nextPage <- None
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
    #if DEBUG 
    theta.StartInfo.WorkingDirectory <- "/home/jinn"
    #else
    theta.StartInfo.WorkingDirectory <- "/home/ec2-user"
    #endif
    theta.StartInfo.FileName <- "java"
    theta.StartInfo.Arguments <- "-jar ThetaTerminal.jar creds=creds"
    theta.Start () |> ignore
    
  member this.Proc = theta

type Theta () =
  let mutable thetaProc = ThetaProc ()

  let resetTheta = MailboxProcessor.Start (fun inbox ->
    let mutable lastTime = DateTime.Now
    async {
      while true do
        let! () = inbox.Receive ()

        if (DateTime.Now - lastTime).Seconds > 10
        then
          discord.SendAlert "killing thetadata." |> Async.Start
          let td = thetaProc.Proc
          td.Kill ()
          td.Dispose ()
          thetaProc <- ThetaProc ()

        lastTime <- DateTime.Now
    })
  
  member this.Reset () = resetTheta.Post ()

  member this.Kill () = thetaProc.Proc.Kill ()
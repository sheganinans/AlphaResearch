module Shared.Roots

open System
open System.Net.Http

open Microsoft.FSharp.Core
open SpanJson

open FSharp.Json

open Shared.ThetaData

let getStockRoots () =
  task {
    try
      use client = new HttpClient ()
      client.DefaultRequestHeaders.Add ("Accept", "application/json")
      let! response = client.GetAsync "http://127.0.0.1:25510/list/roots?sec=STOCK"
      let! c = response.Content.ReadAsStringAsync ()
      return Result.Ok (JsonSerializer.Generic.Utf16.Deserialize<Rsp<string []>> c).response
    with _ -> return Error "failed to get roots"
  } |> Async.AwaitTask |> Async.RunSynchronously

let getStockDates (root : string) =
  task {
    try
      use client = new HttpClient ()
      client.DefaultRequestHeaders.Add ("Accept", "application/json")
      let! response = client.GetAsync "http://127.0.0.1:25510/list/dates/stock/quote?sec=STOCK"
      let! c = response.Content.ReadAsStringAsync ()
      return Result.Ok (JsonSerializer.Generic.Utf16.Deserialize<Rsp<int []>> c).response
    with _ -> return Error $"failed to get dates for: {root}"
  } |> Async.AwaitTask |> Async.RunSynchronously

type ContractRes =
 | HasData of OptionDescrip []
 | NoData

type HeaderFs = 
  {
    id : int
    latency_ms : int
    error_type : string option
    error_msg  : string option
    next_page  : string option
    format     : string [] option
  }

type RspFs<'t> =
  {
    header   : HeaderFs
    response : 't
  }

let getContracts (d : DateTime) =
  task {
    try
      use client = new HttpClient ()
      client.DefaultRequestHeaders.Add ("Accept", "application/json")
      let! response = client.GetAsync $"http://127.0.0.1:25510/list/contracts/option/trade?start_date=%04i{d.Year}%02i{d.Month}%02i{d.Day}"
      let! c = response.Content.ReadAsStringAsync ()
      return
        try
          Result.Ok
            ((Json.deserialize<RspFs<(string * int * int * string) []>> c).response
             |> Array.map (fun (r,e,s,ri) -> { Day = d; Root = r; Exp = e; Strike = s; Right = ri })
             |> ContractRes.HasData)
        with err ->
          try
            let err = (Json.deserialize<RspFs<int []>> c).header
            match err.error_type with
            | Some "NO_DATA" -> Result.Ok NoData
            | _ -> Error $"getContracts3: {err.error_type}"
          with err -> Error $"getContracts4: {err}"
    with err -> return Error $"getContracts2: {err}"
  } |> Async.AwaitTask |> Async.RunSynchronously


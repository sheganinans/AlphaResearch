module Geddit.Roots

open System
open System.Net.Http

open SpanJson

open Geddit.ThetaData

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

let getContracts (d : DateTime) =
  task {
    try
      use client = new HttpClient ()
      client.DefaultRequestHeaders.Add ("Accept", "application/json")
      let! response = client.GetAsync $"http://127.0.0.1:25510/list/contracts/option/trade?start_date=%04i{d.Year}%02i{d.Month}%02i{d.Day}"
      let! c = response.Content.ReadAsStringAsync ()
      return Result.Ok (JsonSerializer.Generic.Utf16.Deserialize<Rsp<Tuple<string, int, int, string> []>> c).response
    with _ -> return Error "failed to get contracts"
  } |> Async.AwaitTask |> Async.RunSynchronously
  

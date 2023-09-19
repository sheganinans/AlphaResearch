open System
open ClickHouse.Client.ADO
open Dapper

let pw = Environment.GetEnvironmentVariable "CK_PW"

using (new ClickHouseConnection ($"Host=127.0.0.1;Password={pw}")) (fun c ->
  let r = c.QueryAsync<int> "select count() from thetadata_stock_trade_quotes prewhere ticker == 'NVDA'" |> Async.AwaitTask |> Async.RunSynchronously
  printfn $"{r}"  )
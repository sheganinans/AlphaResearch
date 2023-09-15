module Shared.Discord

open System
open System.Threading
open Discord
open Discord.WebSocket

type Discord () =
  let discord = new DiscordSocketClient ()
  let user_id = Environment.GetEnvironmentVariable "DISCORD_USER_ID"
  let token =  Environment.GetEnvironmentVariable "DISCORD_TOKEN"
  let guild = uint64 <| Environment.GetEnvironmentVariable "DISCORD_GUILD"
  let channel = uint64 <| Environment.GetEnvironmentVariable "DISCORD_CHANNEL"
  do
    printfn "logging into discord."
    task {
      do! discord.LoginAsync (TokenType.Bot, token)
      do! discord.StartAsync ()
    } |> Async.AwaitTask |> Async.RunSynchronously

  member this.SendAlert (m : string) =
    async {
      printfn $"%s{m}"
      do!
        discord.GetGuild(guild).GetTextChannel(channel).SendMessageAsync $"<@{user_id}>\n```{m[..1950]}\n```"
        |> Async.AwaitTask
        |> Async.Ignore
    }

  member this.SendNotification (m : string) =
    async {
      do!
        discord.GetGuild(guild).GetTextChannel(channel).SendMessageAsync $"```{m[..1800]}\n```"
        |> Async.AwaitTask
        |> Async.Ignore
    }

type private SyncRoot = class end

type Singleton =
  [<DefaultValue>] static val mutable private instance: Discord

  private new () = { new Singleton }

  static member Instance = 
    lock typeof<SyncRoot> (fun () ->
      if box Singleton.instance = null then
        Singleton.instance <- Discord ()
        Thread.Sleep 7_000)
    Singleton.instance    

let discord = Singleton.Instance
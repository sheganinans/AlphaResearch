module Geddit.Wasabi

open System
open System.IO
open Amazon.S3
open Amazon.S3.Transfer

let private config = AmazonS3Config  ()
config.ServiceURL <- "https://s3.wasabisys.com"
let key = Environment.GetEnvironmentVariable "WASABI_KEY"
let secret = Environment.GetEnvironmentVariable "WASABI_SECRET"
let private s3 = new AmazonS3Client (key, secret, config)

let uploadStream (file : Stream) (bucket : string) (key : string) =
  use u = new TransferUtility (s3)
  u.Upload (file, bucket, key)

let uploadPath (file : string) (bucket : string) (key : string) =
  use u = new TransferUtility (s3)
  u.UploadAsync (file, bucket, key) |> Async.AwaitTask |> Async.RunSynchronously
  u.Dispose ()

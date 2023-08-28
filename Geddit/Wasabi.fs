module Geddit.Wasabi

open System
open Amazon.S3
open Amazon.S3.Transfer

let private config = AmazonS3Config  ()
config.ServiceURL <- "https://s3.wasabisys.com"
let key = Environment.GetEnvironmentVariable "WASABI_KEY"
let secret = Environment.GetEnvironmentVariable "WASABI_SECRET"
let private s3 = new AmazonS3Client (key, secret, config)

let uploadFile (file : string) (bucket : string) (key : string) =
  use u = new TransferUtility (s3)
  u.Upload (file, bucket, key)
  u.Dispose ()
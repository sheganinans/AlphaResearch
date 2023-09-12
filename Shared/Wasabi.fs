module Shared.Wasabi

open System
open System.IO
open Amazon.S3
open Amazon.S3.Model
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

let downloadFile (file : string) (bucket : string) (key : string) =
  use u = new TransferUtility (s3)
  u.DownloadAsync (file, bucket, key) |> Async.AwaitTask |> Async.RunSynchronously
  u.Dispose ()

let deleteFile (bucket : string) (key : string) =
  use u = new TransferUtility (s3)
  u.S3Client.DeleteObjectAsync (bucket, key) |> Async.AwaitTask |> Async.Ignore |> Async.RunSynchronously

let getWasabiObjs (bucket : string) (prefix : string) =
  let req = ListObjectsRequest ()
  req.BucketName <- bucket
  req.Prefix <- prefix
  let mutable rsp = s3.ListObjectsAsync req |> Async.AwaitTask |> Async.RunSynchronously
  seq {
    yield! rsp.S3Objects
    while rsp.IsTruncated do
      req.Marker <- rsp.NextMarker
      rsp <- s3.ListObjectsAsync req |> Async.AwaitTask |> Async.RunSynchronously
      yield! rsp.S3Objects
  }
  
let deleteDir (bucket : string) (prefix : string) =
  let objs = getWasabiObjs bucket prefix
  use u = new TransferUtility (s3)
  for o in objs do
    u.S3Client.DeleteObjectAsync (bucket, o.Key)
    |> Async.AwaitTask 
    |> Async.Ignore 
    |> Async.RunSynchronously

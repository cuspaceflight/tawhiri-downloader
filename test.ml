open Core.Std
open Async.Std
open Cohttp
open Cohttp_async
open Common

let uri_prefix = "http://ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.2015080106/gfs.t06z.pgrb2b.0p50.f159"

let (>>|?) = `no
let (>>=?) = `no
let `no = (>>|?)
let `no = (>>=?)

(* please forgive me. *)
let (>>=?=) x f =
  x >>= function
  | Ok y -> f y
  | (Error _ as e) -> return e
let (>>|?=) x f = 
  x >>| fun x ->
  Result.bind x f
let (>>=?|) x f =
  x >>= function
  | Ok x ->
    f x >>| fun x ->
    Ok x
  | (Error _ as e) -> return e
let (>>|?|) x f =
  x >>| fun x ->
  Result.map x ~f

let throttled_download =
  let throttle = Throttle.create ~continue_on_error:true ~max_concurrent_jobs:5 in
  fun uri ~timeout ~progress ~range ->
    Throttle.enqueue throttle (fun () ->
      Download.get uri ~timeout ~progress ~range
    )

let test () =
  throttled_download
    (Uri.of_string (uri_prefix ^ ".idx"))
    ~timeout:(Time.Span.of_sec 10.)
    ~progress:ignore
    ~range:(`all_with_max_len (32 * 1024))
  >>|?| Bigstring.to_string
  >>|?= Grib_index.parse
  >>=?= fun messages ->
  Deferred.List.map messages ~how:`Sequential ~f:(fun msg ->
    let { Grib_index.offset; length; forecast; variable; level; hour } = msg in
    throttled_download
      (Uri.of_string uri_prefix)
      ~timeout:(Time.Span.of_sec 10.)
      ~range:(`exactly_pos_len (offset, length))
      ~progress:ignore
    >>|?= Grib_api.of_bigstring
    >>|?= fun handle ->
    let matches =
         Grib_api.variable handle = Ok variable
      && Grib_api.layout handle   = Ok Layout.Half_deg
      && Grib_api.level handle    = Ok level
      && Grib_api.hour handle     = Ok hour
      && forecast = (Date.of_string "2015-08-01", `h06)
    in
    if matches
    then begin
      Log.Global.info !"Got %{Variable} %{Level} %i" variable level hour;
      Ok (variable, level, hour)
    end
    else Error (Error.of_string "index/message mismatch")
  )
  >>| Or_error.combine_errors

let cmd = 
  Command.async
    ~summary:"Test"
    Command.Spec.empty
    (fun () ->
      test () 
      >>= fun r ->
      begin
        match r with
        | Ok _ -> Log.Global.info "all ok";
        | Error e -> Error.raise e
      end;
      Log.Global.flushed ()
    )

let () = Command.run cmd

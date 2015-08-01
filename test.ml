open Core.Std
open Async.Std
open Cohttp
open Cohttp_async

let uri = Uri.of_string "http://ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.2015080106/gfs.t06z.pgrb2b.0p50.f159"
let offset = 6364853
let length = 6496803 - 6364853

let test () =
  Download.get
    uri
    ~timeout:(Time.Span.of_sec 10.)
    ~range:(`exactly_pos_len (offset, length))
    ~progress:(Log.Global.info "Got %i")
  >>=? Fn.compose return Grib_api.of_bigstring
  >>=? fun handle ->
  return (
    let open Result.Monad_infix in
    Grib_api.variable handle >>= fun variable ->
    Grib_api.layout handle >>= fun `half_deg ->
    Grib_api.level handle >>= fun level ->
    Grib_api.hour handle >>| fun hour ->
    (variable, hour, level)
  )

let cmd = 
  Command.async
    ~summary:"Test"
    Command.Spec.empty
    (fun () ->
      test () 
      >>= fun r ->
      begin
        match r with
        | Ok (variable, hour, level) ->
          Log.Global.info
            !"got %{Sexp} %i %i"
            (<:sexp_of< [`height|`u_wind|`v_wind] >> variable)
            hour level;
        | Error e -> Error.raise e
      end;
      Log.Global.flushed ()
    )

let () = Command.run cmd

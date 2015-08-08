open Core.Std
open Async.Std
open Common

module Urls = struct
  let base_url = "http://www.nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
  let forecast_dir = sprintf !"%s/gfs.%{Forecast_time.to_string_noaa}/" base_url

  let grib_file fcst_time levels hour =
    let fcst_hr = Forecast_time.hour_int fcst_time in
    let maybe_b =
      match (levels : Level_set.t) with
      | A -> ""
      | B -> "b"
    in
    forecast_dir fcst_time ^ sprintf "gfs.t%02iz.pgrb2%s.0p50.f%03i" fcst_hr maybe_b (Hour.to_int hour)

  let index_file fcst_time levels hour =
    grib_file fcst_time levels hour ^ ".idx"

  let index_file fcst_time levels hour = Uri.of_string (index_file fcst_time levels hour)
  let grib_file  fcst_time levels hour = Uri.of_string (grib_file  fcst_time levels hour)
end

let throttled_get =
  let throttle = Throttle.create ~continue_on_error:true ~max_concurrent_jobs:5 in
  fun uri ~interrupt ~range ->
    Throttle.enqueue throttle (fun () ->
      Http.get uri ~interrupt ~range
    )

let with_retries ~name ~f ~attempt_timeout ~interrupt =
  let next_backoff x =
    let open Time.Span in 
    if x < of_sec 100.
    then scale x 2.
    else x
  in
  let rec loop ~backoff =
    let interrupt_this = Ivar.create () in
    let res = f ~interrupt:(Ivar.read interrupt_this) () in
    choose
      [ choice res (fun res -> `Res res)
      ; choice (Clock.after attempt_timeout) (fun () -> `Timeout)
      ; choice interrupt (fun () -> `Interrupted)
      ]
    >>= fun res ->
    Ivar.fill interrupt_this ();
    let retry () =
      Clock.after backoff >>= fun () ->
      loop ~backoff:(next_backoff backoff)
    in
    match res with
    | `Res (Ok res) ->
      Log.Global.debug "%s OK" name;
      return (Ok res)
    | `Res (Error err) ->
      Log.Global.debug !"%s %{Error#mach} (backoff %{Time.Span})" name err backoff;
      retry ()
    | `Timeout ->
      Log.Global.debug !"%s Timeout (backoff %{Time.Span})" name backoff;
      retry ()
    | `Interrupted ->
      return (Or_error.error_string "interrupted")
  in
  loop ~backoff:(Time.Span.of_sec 5.)

let check_index_has_all_messages =
  let expect_count =
    let f ls = (List.length Variable.axis) * (List.length (Level.ts_in ls)) in
    let a = f Level_set.A in
    let b = f Level_set.B in
    function
    | Level_set.A -> a
    | Level_set.B -> b
  in
  let contains_dup =
    let open Grib_index in
    let compare a b =
      <:compare< Variable.t * Level.t >>
        (a.variable, a.level)
        (b.variable, b.level)
    in
    List.contains_dup ~compare
  in
  fun messages ~level_set ~expect_hour ~expect_fcst_time ->
    let open Result.Monad_infix in
    let rec check_subset =
      function
      | [] -> Ok ()
      | msg :: messages ->
        let { Grib_index.offset = _; length = _; fcst_time; variable = _; level; hour } = msg in
        if fcst_time <> expect_fcst_time || hour <> expect_hour || Level.level_set level <> level_set
        then
          Or_error.errorf !"unexpected message %{Grib_index.message_to_string}" msg
        else
          check_subset messages
    in
    check_subset messages >>= fun () ->
    if contains_dup messages
    then Or_error.error_string "Duplicate messages in index"
    else begin
      let count = List.length messages in
      assert (count <= expect_count level_set);
      if count <> expect_count level_set
      then Or_error.error_string "Messages missing from index"
      else Ok ()
    end

let get_index ~interrupt fcst_time level_set hour =
  with_retries
    ~name:(
      sprintf
        !"Download index %{Forecast_time} %{Level_set} %{Hour}"
        fcst_time level_set hour
    )
    ~interrupt ~attempt_timeout:(Time.Span.of_sec 10.)
    ~f:(fun ~interrupt () ->
      let open Deferred_result_infix in
      throttled_get
        (Urls.index_file fcst_time level_set hour)
        ~range:(`all_with_max_len (32 * 1024))
        ~interrupt
      >>|?| Bigstring.to_string
      >>|?= Grib_index.parse
      >>|?= fun messages ->
      check_index_has_all_messages 
        messages
        ~level_set
        ~expect_fcst_time:fcst_time
        ~expect_hour:hour
      >>-?| fun () ->
      messages
    )

let get_message ~interrupt (msg : Grib_index.message) =
  with_retries
    ~name:("Download message " ^ Grib_index.message_to_string msg)
    ~interrupt ~attempt_timeout:(Time.Span.of_sec 60.)
    ~f:(fun ~interrupt () ->
      let open Deferred_result_infix in
      throttled_get
        (Urls.grib_file msg.fcst_time (Level.level_set msg.level) msg.hour)
        ~range:(`exactly_pos_len (msg.offset, msg.length))
        ~interrupt
      >>|?= Grib_message.of_bigstring
      >>|?= fun message ->
      let matches =
           Grib_message.variable message = Ok msg.variable
        && Grib_message.hour     message = Ok msg.hour
        && Grib_message.level    message = Ok msg.level
        && Grib_message.layout   message = Ok Layout.Half_deg
      in
      if matches
      then Ok message
      else Or_error.errorf "GRIB message contents did not match index"
    )

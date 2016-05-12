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

let interrupted_error = return (Or_error.error_string "interrupted")

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
      choose
        [ choice (Clock.after backoff) (fun () -> `Ready)
        ; choice interrupt (fun () -> `Interrupted)
        ]
      >>= function
      | `Ready -> loop ~backoff:(next_backoff backoff)
      | `Interrupted -> interrupted_error
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
      interrupted_error
  in
  loop ~backoff:(Time.Span.of_sec 5.)

let filter_messages_and_assert_all_present =
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
      [%compare: Variable.t * Level.t]
        (a.variable, a.level)
        (b.variable, b.level)
    in
    List.contains_dup ~compare
  in
  fun messages ~level_set ~expect_hour ~expect_fcst_time ->
    let open Result.Monad_infix in
    let rec filter_messages ~acc ~messages =
      match messages with
      | [] -> Ok acc
      | msg :: messages ->
        let { Grib_index.offset = _; length = _; fcst_time; variable = _; level; hour } = msg in
        if fcst_time <> expect_fcst_time || hour <> expect_hour
        then Or_error.errorf !"unexpected message %{Grib_index.message_to_string}" msg
        else if Level.level_set level = level_set
        then filter_messages ~acc:(msg :: acc) ~messages
        else filter_messages ~acc ~messages
    in
    filter_messages ~acc:[] ~messages
    >>= fun messages ->
    if contains_dup messages
    then Or_error.error_string "Duplicate messages in index"
    else begin
      let count = List.length messages in
      assert (count <= expect_count level_set);
      if count <> expect_count level_set
      then Or_error.error_string "Messages missing from index"
      else Ok messages
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
      filter_messages_and_assert_all_present
        messages
        ~level_set
        ~expect_fcst_time:fcst_time
        ~expect_hour:hour
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

let download_raw ~interrupt ~filename fcst_time =
  let open Deferred_result_infix in
  Log.Global.debug !"Begin download of %{Forecast_time}" fcst_time;
  Dataset_file.create ~filename RW
  >>=?= fun ds ->
  let maybe_wait =
    function
    | Some x -> x
    | None -> return (Ok ())
  in
  let message_job ~interrupt msg =
    get_message ~interrupt msg
    >>=?= fun grib ->
    let module G = Grib_index in
    let slice = Dataset_file.slice ds msg.G.hour msg.G.level msg.G.variable in
    In_thread.run ~name:"blit" (fun () -> Grib_message.blit grib slice)
    >>|?| fun () ->
    Log.Global.debug !"Blitted %{Grib_index.message_to_string}" msg
  in
  let index_job (hour, level_set) =
    let interrupt_this = Ivar.create () in
    upon interrupt (fun () -> Ivar.fill_if_empty interrupt_this ());
    let interrupt = Ivar.read interrupt_this in
    get_index ~interrupt fcst_time level_set hour
    >>=?= fun messages ->
    let (last, rest) =
      match List.rev messages with
      | last :: rest -> (last, List.rev rest)
      | [] -> assert false
    in
    (* wait for the last message in the file to complete first, so that
     * we know the whole file is there. *)
    message_job ~interrupt last
    >>=?= fun () ->
    let rest_results = List.map rest ~f:(message_job ~interrupt) in
    let error_early_warning =
      Deferred.create (fun ivar ->
        List.iter rest_results ~f:(fun res ->
          res
          >>> function
          | Ok _ -> ()
          | Error e -> Ivar.fill_if_empty ivar e
        )
      )
    in
    choose
      [ choice (Deferred.all rest_results) Or_error.combine_errors_unit
      ; choice error_early_warning (fun x -> Error x)
      ]
    >>| fun final ->
    Ivar.fill_if_empty interrupt_this ();
    final
  in
  let rec loop ~ongoing1 ~ongoing2 ~waiting =
    (* The files are released in order, so wait for the one two-before this one
     * to be done before trying the next. *)
    match waiting with
    | [] ->
      maybe_wait ongoing1 >>=?= fun () ->
      maybe_wait ongoing2 >>=?= fun () ->
      return (Ok ())
    | hour_level_set :: waiting ->
      maybe_wait ongoing1 >>=?= fun () ->
      let ongoing1 = ongoing2 in
      let ongoing2 = Some (index_job hour_level_set) in
      loop ~ongoing1 ~ongoing2 ~waiting
  in
  let jobs =
    List.cartesian_product Hour.axis Level_set.([A; B])
    |> List.sort ~cmp:(fun (h1, _) (h2, _) -> Hour.compare h1 h2)
  in
  loop ~ongoing1:None ~ongoing2:None ~waiting:jobs
  >>=?= fun () ->
  Dataset_file.msync ds

let download ~interrupt ?directory forecast_time =
  let open Deferred_result_infix in
  let (temp_filename, final_filename) =
    let open Dataset_file.Filename in
    ( one ?directory ~prefix:downloader_prefix forecast_time
    , one ?directory forecast_time
    )
  in
  Log.Global.debug "Temp filename will be %s" temp_filename;
  begin
    download_raw ~interrupt ~filename:temp_filename forecast_time
    >>=?= fun () ->
    Log.Global.debug "Renaming %s -> %s" temp_filename final_filename;
    Monitor.try_with_or_error (fun () -> Sys.rename temp_filename final_filename)
  end
  >>= function
  | Ok () as ok -> return ok
  | Error _ as dl_err ->
    Monitor.try_with_or_error (fun () -> Sys.remove temp_filename)
    >>= fun delete_res ->
    begin
      match delete_res with
      | Ok () -> Log.Global.debug "Deleted %s" temp_filename
      | Error e -> Log.Global.debug !"Failed to delete temp file %s: %{Error#mach}" temp_filename e
    end;
    return dl_err

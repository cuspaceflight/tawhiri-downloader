open Core
open Async
open Common

module Urls = struct
  let base_url = "https://www.nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
  let forecast_dir = sprintf !"%s/gfs.%{Forecast_time#yyyymmdd_slash_hh}/" base_url

  let grib_file fcst_time levels hour =
    let fcst_hr = Forecast_time.hour_int fcst_time in
    let maybe_b =
      match (levels : Level_set.t) with
      | A -> ""
      | B -> "b"
    in
    forecast_dir fcst_time
    ^ sprintf "gfs.t%02iz.pgrb2%s.0p50.f%03i" fcst_hr maybe_b (Hour.to_int hour)
  ;;

  let index_file fcst_time levels hour = grib_file fcst_time levels hour ^ ".idx"
  let index_file fcst_time levels hour = index_file fcst_time levels hour
  let grib_file fcst_time levels hour = grib_file fcst_time levels hour
end

let throttled_get =
  let throttle = Throttle.create ~continue_on_error:true ~max_concurrent_jobs:5 in
  fun uri ~interrupt ~range ->
    Throttle.enqueue throttle (fun () -> Http.get uri ~interrupt ~range)
;;

let interrupted_error = return (Or_error.error_string "interrupted")

let with_retries ~name ~f ~attempt_timeout ~interrupt =
  let next_backoff x =
    let open Time.Span in
    if x < of_sec 100. then scale x 2. else x
  in
  let rec loop ~backoff =
    let interrupt_this = Ivar.create () in
    let res = f ~interrupt:(Ivar.read interrupt_this) () in
    let%bind res =
      choose
        [ choice res (fun res -> `Res res)
        ; choice (Clock.after attempt_timeout) (fun () -> `Timeout)
        ; choice interrupt (fun () -> `Interrupted)
        ]
    in
    Ivar.fill interrupt_this ();
    let retry () =
      match%bind
        choose
          [ choice (Clock.after backoff) (fun () -> `Ready)
          ; choice interrupt (fun () -> `Interrupted)
          ]
      with
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
    | `Interrupted -> interrupted_error
  in
  loop ~backoff:(Time.Span.of_sec 5.)
;;

let filter_messages_and_assert_all_present =
  let expect_count =
    let f ls = List.length Variable.axis * List.length (Level.ts_in ls) in
    let a = f Level_set.A in
    let b = f Level_set.B in
    function
    | Level_set.A -> a
    | Level_set.B -> b
  in
  let contains_dup =
    let open Grib_index in
    let compare a b =
      [%compare: Variable.t * Level.t] (a.variable, a.level) (b.variable, b.level)
    in
    List.contains_dup ~compare
  in
  fun messages ~level_set ~expect_hour ~expect_fcst_time ->
    let rec filter_messages ~acc ~messages =
      match messages with
      | [] -> Ok acc
      | msg :: messages ->
        let { Grib_index.offset = _; length = _; fcst_time; variable = _; level; hour } =
          msg
        in
        let time_and_hour_correct =
          [%equal: Forecast_time.t] fcst_time expect_fcst_time
          && [%compare.equal: Hour.t] hour expect_hour
        in
        if not time_and_hour_correct
        then Or_error.errorf !"unexpected message %{Grib_index.message_to_string}" msg
        else if [%compare.equal: Level_set.t] (Level.level_set level) level_set
        then filter_messages ~acc:(msg :: acc) ~messages
        else filter_messages ~acc ~messages
    in
    match filter_messages ~acc:[] ~messages with
    | Error _ as error -> error
    | Ok messages ->
      if contains_dup messages
      then Or_error.error_string "Duplicate messages in index"
      else (
        let count = List.length messages in
        assert (count <= expect_count level_set);
        if count <> expect_count level_set
        then Or_error.error_string "Messages missing from index"
        else Ok messages)
;;

let get_index ~interrupt fcst_time level_set hour =
  with_retries
    ~name:
      (sprintf
         !"Download index %{Forecast_time#yyyymmddhh} %{Level_set} %{Hour}"
         fcst_time
         level_set
         hour)
    ~interrupt
    ~attempt_timeout:(Time.Span.of_sec 10.)
    ~f:(fun ~interrupt () ->
      match%bind
        throttled_get
          (Urls.index_file fcst_time level_set hour)
          ~range:(`all_with_max_len (128 * 1024))
          ~interrupt
      with
      | Error _ as error -> return error
      | Ok bigstring ->
        (match Grib_index.parse (Bigstring.to_string bigstring) with
        | Error _ as error -> return error
        | Ok messages ->
          let checked_messages =
            filter_messages_and_assert_all_present
              messages
              ~level_set
              ~expect_fcst_time:fcst_time
              ~expect_hour:hour
          in
          return checked_messages))
;;

let get_message ~interrupt (msg : Grib_index.message) =
  with_retries
    ~name:("Download message " ^ Grib_index.message_to_string msg)
    ~interrupt
    ~attempt_timeout:(Time.Span.of_sec 60.)
    ~f:(fun ~interrupt () ->
      match%bind
        throttled_get
          (Urls.grib_file msg.fcst_time (Level.level_set msg.level) msg.hour)
          ~range:(`exactly_pos_len (msg.offset, msg.length))
          ~interrupt
      with
      | Error _ as error -> return error
      | Ok bigstring ->
        (match Grib_message.of_bigstring bigstring with
        | Error _ as error -> return error
        | Ok message ->
          let matches =
            [%compare.equal: Variable.t Or_error.t]
              (Grib_message.variable message)
              (Ok msg.variable)
            && [%compare.equal: Hour.t Or_error.t]
                 (Grib_message.hour message)
                 (Ok msg.hour)
            && [%compare.equal: Level.t Or_error.t]
                 (Grib_message.level message)
                 (Ok msg.level)
            && [%compare.equal: Layout.t Or_error.t]
                 (Grib_message.layout message)
                 (Ok Half_deg)
          in
          if matches
          then return (Ok message)
          else return (Or_error.errorf "GRIB message contents did not match index")))
;;

let download_to_temp_filename ~interrupt ~temp_filename fcst_time =
  Log.Global.debug !"Begin download of %{Forecast_time#yyyymmddhh}" fcst_time;
  match%bind Dataset_file.create ~filename:temp_filename RW with
  | Error _ as error -> return error
  | Ok ds ->
    let message_job ~interrupt msg =
      match%bind get_message ~interrupt msg with
      | Error _ as error -> return error
      | Ok grib ->
        let slice = Dataset_file.slice ds msg.hour msg.level msg.variable in
        (match%bind
           In_thread.run ~name:"blit" (fun () -> Grib_message.blit grib slice)
         with
        | Error _ as error -> return error
        | Ok () ->
          Log.Global.debug !"Blitted %{Grib_index.message_to_string}" msg;
          return (Ok ()))
    in
    let do_one_job (hour, level_set) =
      let interrupt_this = Ivar.create () in
      upon interrupt (fun () -> Ivar.fill_if_empty interrupt_this ());
      let interrupt = Ivar.read interrupt_this in
      match%bind get_index ~interrupt fcst_time level_set hour with
      | Error _ as error -> return error
      | Ok messages ->
        let last, rest =
          match List.rev messages with
          | last :: rest -> last, List.rev rest
          | [] -> assert false
        in
        (* wait for the last message in the file to complete first, so that
           we know the whole file is there. *)
        (match%bind message_job ~interrupt last with
        | Error _ as error -> return error
        | Ok () ->
          let rest_results = List.map rest ~f:(message_job ~interrupt) in
          let error_early_warning =
            (* becomes determined as soon as _any_ job fails, since we want to eagerly kill
               the others at that point *)
            Deferred.create (fun ivar ->
                List.iter rest_results ~f:(fun res ->
                    upon res (fun res ->
                        match res with
                        | Ok _ -> ()
                        | Error e -> Ivar.fill_if_empty ivar e)))
          in
          let%bind final_result =
            choose
              [ choice (Deferred.all rest_results) Or_error.combine_errors_unit
              ; choice error_early_warning (fun x -> Error x)
              ]
          in
          Ivar.fill_if_empty interrupt_this ();
          return final_result)
    in
    let rec loop ~in_flight_job1 ~in_flight_job2 ~waiting_jobs =
      (* The files are released in order, so don't start job N+2 until job N is complete. *)
      match%bind in_flight_job1 with
      | Error _ as error -> return error
      | Ok () ->
        (match waiting_jobs with
        | [] -> in_flight_job2
        | next_job :: waiting_jobs ->
          loop
            ~in_flight_job1:in_flight_job2
            ~in_flight_job2:(do_one_job next_job)
            ~waiting_jobs)
    in
    let jobs =
      List.cartesian_product Hour.axis Level_set.[ A; B ]
      |> List.sort ~compare:(fun (h1, _) (h2, _) -> Hour.compare h1 h2)
    in
    (match%bind
       loop
         ~in_flight_job1:(return (Ok ()))
         ~in_flight_job2:(return (Ok ()))
         ~waiting_jobs:jobs
     with
    | Error _ as error -> return error
    | Ok () -> Dataset_file.msync ds)
;;

let download ~interrupt ?directory forecast_time =
  let make_filename ?prefix () =
    Dataset_file.Filename.one ?directory ?prefix forecast_time
  in
  let temp_filename = make_filename ~prefix:Dataset_file.Filename.downloader_prefix () in
  let final_filename = make_filename () in
  Log.Global.debug "Temp filename will be %s" temp_filename;
  match%bind
    download_to_temp_filename ~interrupt ~temp_filename forecast_time
  with
  | Ok () ->
    Log.Global.debug "Renaming %s -> %s" temp_filename final_filename;
    Monitor.try_with_or_error (fun () -> Sys.rename temp_filename final_filename)
  | Error _ as dl_error ->
    let%bind delete_res =
      Monitor.try_with_or_error (fun () -> Sys.remove temp_filename)
    in
    (match delete_res with
    | Ok () -> Log.Global.debug "Deleted %s" temp_filename
    | Error e ->
      Log.Global.debug !"Failed to delete temp file %s: %{Error#mach}" temp_filename e);
    return dl_error
;;

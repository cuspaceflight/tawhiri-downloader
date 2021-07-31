open Core
open Async
open Common

module Base_url = struct
  type t =
    | Nomads
    | Aws_mirror

  let to_string =
    function
    | Nomads -> "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
    | Aws_mirror -> "https://noaa-gfs-bdp-pds.s3.amazonaws.com"

  let throttle =
    let nomads =
      Limiter_async.Throttle.create_exn
        ~concurrent_jobs_target:5
        ~continue_on_error:false
        ~burst_size:10
        ~sustained_rate_per_sec:0.9
        ()
    in
    let aws_mirror =
      Limiter_async.Throttle.create_exn
        ~concurrent_jobs_target:5
        ~continue_on_error:false
        ()
    in
    function
    | Nomads -> nomads
    | Aws_mirror -> aws_mirror
end

module Local_paths = struct
  let forecast_dir = sprintf !"gfs.%{Forecast_time#yyyymmdd_slash_hh}/atmos/"

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

  let index_file fcst_time levels hour =
    grib_file fcst_time levels hour ^ ".idx"
  ;;
end

let interrupted_error = return (Or_error.error_string "interrupted")

let throttled_get_with_retries
        ~name
        ~attempt_timeout
        ~interrupt
        ~base_url
        ~local_path
        ~range
        ~parse
  =
  let throttle = Base_url.throttle base_url in
  let uri = sprintf "%s/%s" (Base_url.to_string base_url) local_path in
  let next_backoff x =
    let open Time.Span in
    if x < of_sec 100. then scale x 2. else x
  in
  let rec loop ~backoff =
    let interrupt_this = Ivar.create () in
    let result : _ Deferred.t =
      Limiter_async.Throttle.enqueue' 
        throttle 
        (fun () ->
          (* Only start the timeout once we're inside the throttle. We don't
             need to wait for [Interrupted] here, because the outer [choose] will, and kill
             the request if it fires. *)
          let got : _ Deferred.t =
            Http.get
              uri
              ~range
              ~interrupt:(Ivar.read interrupt_this)
          in
          choose
            [ choice got (fun res -> `Get_result res)
            ; choice (Clock.after attempt_timeout) (fun () -> `Timeout)
            ])
        ()
    in
    let%bind result =
      choose
        [ choice result (fun x -> `Throttle_result x)
        ; choice interrupt (fun () -> `Interrupted)
        ]
    in
    Ivar.fill interrupt_this ();
    (match result with
     | `Interrupted -> interrupted_error
     | `Throttle_result Aborted ->
       return (Or_error.error_string "throttle aborted")
     | `Throttle_result (Raised exn) -> raise exn
     | `Throttle_result (Ok (`Get_result (Error err))) ->
       Log.Global.debug !"%s get error: %{Error#mach} (backoff %{Time.Span})" name err backoff;
       retry ~backoff
     | `Throttle_result (Ok `Timeout) ->
       Log.Global.debug !"%s Timeout (backoff %{Time.Span})" name backoff;
       retry ~backoff
     | `Throttle_result (Ok (`Get_result (Ok res))) ->
       (match parse res with
        | Error err ->
          Log.Global.debug !"%s parse error: %{Error#mach} (backoff %{Time.Span})" name err backoff;
          retry ~backoff
        | Ok res ->
          Log.Global.debug "%s OK" name;
          return (Ok res)))
  and retry ~backoff =
    match%bind
      choose
        [ choice (Clock.after backoff) (fun () -> `Ready)
        ; choice interrupt (fun () -> `Interrupted)
        ]
    with
    | `Ready -> loop ~backoff:(next_backoff backoff)
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

let get_index ~interrupt base_url fcst_time level_set hour =
  throttled_get_with_retries
    ~name:
      (sprintf
         !"Download index %{Forecast_time#yyyymmddhh} %{Level_set} %{Hour}"
         fcst_time
         level_set
         hour)
    ~interrupt
    ~attempt_timeout:(Time.Span.of_sec 10.)
    ~base_url
    ~local_path:(Local_paths.index_file fcst_time level_set hour)
    ~range:(`all_with_max_len (128 * 1024))
    ~parse:(fun bigstring ->
      match Grib_index.parse (Bigstring.to_string bigstring) with
      | Error _ as error -> error
      | Ok messages ->
        filter_messages_and_assert_all_present
          messages
          ~level_set
          ~expect_fcst_time:fcst_time
          ~expect_hour:hour)
;;

let get_message ~interrupt base_url (msg : Grib_index.message) =
  throttled_get_with_retries
    ~name:("Download message " ^ Grib_index.message_to_string msg)
    ~interrupt
    ~attempt_timeout:(Time.Span.of_sec 60.)
    ~base_url
    ~local_path:(Local_paths.grib_file msg.fcst_time (Level.level_set msg.level) msg.hour)
    ~range:(`exactly_pos_len (msg.offset, msg.length))
    ~parse:(fun bigstring ->
      match Grib_message.of_bigstring bigstring with
      | Error _ as error -> error
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
        then Ok message
        else Or_error.errorf "GRIB message contents did not match index")
;;

let download_to_temp_filename ~interrupt ~temp_filename base_url fcst_time =
  Log.Global.debug !"Begin download of %{Forecast_time#yyyymmddhh}" fcst_time;
  match%bind Dataset_file.create ~filename:temp_filename RW with
  | Error _ as error -> return error
  | Ok ds ->
    let message_job ~interrupt msg =
      match%bind get_message ~interrupt base_url msg with
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
      match%bind get_index ~interrupt base_url fcst_time level_set hour with
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

let download ~interrupt ?directory base_url forecast_time =
  let make_filename ?prefix () =
    Dataset_file.Filename.one ?directory ?prefix forecast_time
  in
  let temp_filename = make_filename ~prefix:Dataset_file.Filename.downloader_prefix () in
  let final_filename = make_filename () in
  Log.Global.debug "Temp filename will be %s" temp_filename;
  match%bind download_to_temp_filename ~interrupt ~temp_filename base_url forecast_time with
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

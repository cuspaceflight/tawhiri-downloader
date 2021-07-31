open Core
open Async

let one_main ?directory ?log_level base_url forecast_time =
  Option.iter log_level ~f:Log.Global.set_level;
  let interrupt =
    let signal = Ivar.create () in
    Signal.handle Signal.[ int; term ] ~f:(Ivar.fill_if_empty signal);
    let%bind reason =
      choose
        [ choice (Clock.after (Time.Span.of_hr 2.)) (fun () -> "deadline")
        ; choice (Ivar.read signal) Signal.to_string
        ]
    in
    Log.Global.error "Interrupt: %s" reason;
    Deferred.unit
  in
  let%bind res = Download.download ~interrupt ?directory base_url forecast_time in
  let%bind () = Log.Global.flushed () in
  return (Or_error.ok_exn res)
;;

let clean_directory ?directory ~keep () =
  let%bind to_remove =
    match%bind
      Dataset_file.Filename.list
        ?directory
        ~prefix:Dataset_file.Filename.downloader_prefix
        ()
    with
    | Error _ as error -> return error
    | Ok temp_files ->
      (match%bind Dataset_file.Filename.list ?directory () with
      | Error _ as error -> return error
      | Ok ds_times ->
        let files_to_remove =
          List.filter ds_times ~f:(fun x -> not (Forecast_time.equal x.fcst_time keep))
          @ temp_files
        in
        return (Ok files_to_remove))
  in
  match to_remove with
  | Error _ as error -> return error
  | Ok to_remove ->
    let%bind results =
      Deferred.List.map to_remove ~f:(fun { path; _ } ->
          Monitor.try_with_or_error (fun () -> Sys.remove path))
    in
    return (Or_error.combine_errors_unit results)
;;

let send_mail, wait_for_mails =
  let in_flight_mails = Bag.create () in
  let send_mail ~error_rcpt_to message =
    match error_rcpt_to with
    | [] -> ()
    | error_rcpt_to ->
      let res =
        match%bind
          Smtp.send_mail
            ~rcpt_to:error_rcpt_to
            ~subject:"Tawhiri downloader"
            ~data:message
            ()
        with
        | Ok () -> return ()
        | Error err ->
          Log.Global.error !"Ignoring failure to email: %{Error#hum}" err;
          return ()
      in
      let elt = Bag.add in_flight_mails res in
      upon res (fun () -> Bag.remove in_flight_mails elt)
  in
  let wait_for_mails () = Deferred.all_unit (Bag.to_list in_flight_mails) in
  send_mail, wait_for_mails
;;

let daemon_main ?directory ?log_level ?first_fcst_time ~error_rcpt_to ~base_url () =
  Option.iter log_level ~f:Log.Global.set_level;
  let send_mail = send_mail ~error_rcpt_to in
  let first_fcst_time =
    match first_fcst_time with
    | Some x -> x
    | None -> Forecast_time.expect_next_release ()
  in
  let signal_interrupt =
    Deferred.create (fun ivar ->
      Signal.handle
        Signal.[ int; term ]
        ~f:(fun s ->
          let err = Error.of_string (sprintf !"interrupt: %{Signal}" s) in
          Log.Global.info !"Signal received: %{Signal}" s;
          Ivar.fill_if_empty ivar err))
  in
  let%bind error =
    Deferred.repeat_until_finished first_fcst_time (fun forecast_time ->
      let wait_until = Forecast_time.expect_first_file_at forecast_time in
      if Time.( > ) wait_until (Time.now ())
      then
        Log.Global.info
          !"Waiting until %{Time} before starting %{Forecast_time#yyyymmddhh}"
          wait_until
          forecast_time;
      match%bind
        choose
          [ choice (Clock.at wait_until) (fun () -> Ok ())
          ; choice signal_interrupt (fun e -> Error e)
          ]
      with
      | Error error -> return (`Finished error)
      | Ok () ->
        let deadline = Forecast_time.(expect_first_file_at (incr forecast_time)) in
        Log.Global.info
          !"Deadline at %{Time} (in %{Time.Span})"
          deadline
          (Time.diff deadline (Time.now ()));
        let interrupt_this = Ivar.create () in
        let res =
          Download.download
            ~interrupt:(Ivar.read interrupt_this)
            ?directory
            base_url
            forecast_time
        in
        let%bind res =
          choose
            [ choice res (fun x -> `Res x)
            ; choice signal_interrupt (fun x -> `Signal_err x)
            ; choice (Clock.at deadline) (fun () -> `Deadline)
            ]
        in
        Ivar.fill interrupt_this ();
        let goto_next_forecast = return (`Repeat (Forecast_time.incr forecast_time)) in
        (match res with
        | `Signal_err e -> return (`Finished e)
        | `Deadline ->
          let msg =
            sprintf
              !"Deadline for %{Forecast_time#yyyymmddhh} reached, skipping"
              forecast_time
          in
          Log.Global.error "%s" msg;
          send_mail msg;
          goto_next_forecast
        | `Res (Error err) ->
          Log.Global.error
            !"%{Forecast_time#yyyymmddhh} failed: %{Error#mach}"
            forecast_time
            err;
          send_mail
            (sprintf
               !"%{Forecast_time#yyyymmddhh} failed\n\n%{Error#hum}"
               forecast_time
               err);
          goto_next_forecast
        | `Res (Ok ()) ->
          Log.Global.info !"Completed %{Forecast_time#yyyymmddhh}" forecast_time;
          (match%bind clean_directory ?directory ~keep:forecast_time () with
          | Error err ->
            Log.Global.error !"Cleanup failed %{Error#mach}" err;
            return (`Finished err)
          | Ok () -> goto_next_forecast)))
  in
  let%bind () = wait_for_mails () in
  let%bind () = Log.Global.flushed () in
  Error.raise error
;;

type shared_args =
  { log_level : Log.Level.t option
  ; directory : string option
  ; base_url : Download.Base_url.t
  }

let shared_args =
  let log_level_arg =
    Command.Arg_type.create (fun s ->
      match String.lowercase s with
      | "info" -> `Info
      | "debug" -> `Debug
      | "error" -> `Error
      | _ -> failwithf "Invalid log level %s, choose info debug or error" s ())
  and base_url =
    Command.Arg_type.create (fun s : Download.Base_url.t ->
      match String.lowercase s with
      | "nomads" -> Nomads
      | "aws-mirror" -> Aws_mirror
      | _ -> failwithf "Bad url %s, choose nomads or aws-mirror" s ())
  in
  [%map_open.Command
    let directory =
      flag
        "directory"
        (optional Filename.arg_type)
        ~doc:"DIR (optional) directory in which to place the dataset"
    and log_level =
      flag
        "log-level"
        (optional log_level_arg)
        ~doc:"DEBUG|INFO|ERROR (optional) log level"
    and base_url =
      flag
        "base-url"
        (optional_with_default Download.Base_url.Nomads base_url)
        ~doc:"nomads|aws-mirror (optional) select where to download from"
    in
    { directory; log_level; base_url }]
;;

let forecast_time_arg =
  Command.Spec.Arg_type.create (fun s ->
      Or_error.ok_exn (Forecast_time.of_string_yyyymmddhh s))
;;

let one_cmd =
  Command.async
    ~summary:"Download a specific dataset"
    [%map_open.Command
      let { log_level; directory; base_url } = shared_args
      and forecast_time = anon ("forecast_time" %: forecast_time_arg) in
      fun () -> one_main ?log_level ?directory base_url forecast_time]
;;

let daemon_cmd =
  Command.async
    ~summary:"Start the downloader daemon"
    [%map_open.Command
      let { log_level; directory; base_url } = shared_args
      and first_fcst_time =
        flag
          "first-forecast-time"
          (optional forecast_time_arg)
          ~doc:"YYYYMMDDHH first to download"
      and error_rcpt_to =
        flag
          "error-rcpt-to"
          (listed string)
          ~doc:"mail@domain send error notices to this address"
      in
      fun () ->
        daemon_main
          ?log_level
          ?directory
          ?first_fcst_time
          ~error_rcpt_to
          ~base_url
          ()]
;;

let cmd =
  Command.group
    ~summary:"Tawhiri dataset downloader"
    [ "one", one_cmd; "daemon", daemon_cmd ]
;;

let () = Command.run cmd

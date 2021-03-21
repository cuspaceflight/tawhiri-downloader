open Core
open Async
open Common

let one_main ?directory ?log_level forecast_time () =
  Option.iter log_level ~f:Log.Global.set_level;
  let interrupt =
    let signal = Ivar.create () in
    Signal.handle Signal.[ int; term ] ~f:(Ivar.fill_if_empty signal);
    choose
      [ choice (Clock.after (Time.Span.of_hr 2.)) (fun () -> "deadline")
      ; choice (Ivar.read signal) Signal.to_string
      ]
    >>= fun reason ->
    Log.Global.error "Interrupt: %s" reason;
    Deferred.unit
  in
  Download.download ~interrupt ?directory forecast_time
  >>= fun res -> Log.Global.flushed () >>= fun () -> return (Or_error.ok_exn res)
;;

let clean_directory ?directory ~keep () =
  let open Deferred_result_infix in
  (let open Dataset_file.Filename in
  list ?directory ~prefix:downloader_prefix ()
  >>=?= fun temp_files ->
  list ?directory ()
  >>|?| fun ds_times ->
  List.filter ds_times ~f:(fun x -> not (Forecast_time.equal x.fcst_time keep))
  @ temp_files
  |> List.map ~f:(fun x -> x.path))
  >>=?= fun to_remove ->
  Deferred.List.map to_remove ~f:(fun path ->
      Monitor.try_with_or_error (fun () -> Sys.remove path))
  >>| Or_error.combine_errors_unit
;;

let send_mail, wait_for_mails =
  let in_flight_mails = Bag.create () in
  let send_mail ~error_rcpt_to message =
    match error_rcpt_to with
    | [] -> ()
    | error_rcpt_to ->
      let res =
        Smtp.send_mail
          ~rcpt_to:error_rcpt_to
          ~subject:"Tawhiri downloader"
          ~data:message
          ()
        >>| function
        | Error err -> Log.Global.error !"Ignoring failure to email: %{Error#hum}" err
        | Ok () -> ()
      in
      let elt = Bag.add in_flight_mails res in
      upon res (fun () -> Bag.remove in_flight_mails elt)
  in
  let wait_for_mails () = Bag.to_list in_flight_mails |> Deferred.List.iter ~f:Fn.id in
  send_mail, wait_for_mails
;;

let daemon_main ?directory ?log_level ?first_fcst_time ~error_rcpt_to () =
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
  let rec loop forecast_time =
    let open Deferred_result_infix in
    let wait_until = Forecast_time.expect_first_file_at forecast_time in
    if Time.( > ) wait_until (Time.now ())
    then
      Log.Global.info
        !"Waiting until %{Time} before starting %{Forecast_time#yyyymmddhh}"
        wait_until
        forecast_time;
    choose
      [ choice (Clock.at wait_until) (fun () -> Ok ())
      ; choice signal_interrupt (fun e -> Error e)
      ]
    >>=?= fun () ->
    let deadline = Forecast_time.(expect_first_file_at (incr forecast_time)) in
    Log.Global.info
      !"Deadline at %{Time} (in %{Time.Span})"
      deadline
      (Time.diff deadline (Time.now ()));
    let interrupt_this = Ivar.create () in
    let res =
      Download.download ~interrupt:(Ivar.read interrupt_this) ?directory forecast_time
    in
    choose
      [ choice res (fun x -> `Res x)
      ; choice signal_interrupt (fun x -> `Signal_err x)
      ; choice (Clock.at deadline) (fun () -> `Deadline)
      ]
    >>= fun res ->
    Ivar.fill interrupt_this ();
    let continue () = loop (Forecast_time.incr forecast_time) in
    match res with
    | `Signal_err e -> return (Error e)
    | `Deadline ->
      let msg =
        sprintf
          !"Deadline for %{Forecast_time#yyyymmddhh} reached, skipping"
          forecast_time
      in
      Log.Global.error "%s" msg;
      send_mail msg;
      continue ()
    | `Res (Error err) ->
      Log.Global.error
        !"%{Forecast_time#yyyymmddhh} failed: %{Error#mach}"
        forecast_time
        err;
      send_mail
        (sprintf !"%{Forecast_time#yyyymmddhh} failed\n\n%{Error#hum}" forecast_time err);
      continue ()
    | `Res (Ok ()) ->
      Log.Global.info !"Completed %{Forecast_time#yyyymmddhh}" forecast_time;
      clean_directory ?directory ~keep:forecast_time ()
      >>= (function
      | Error err as err' ->
        Log.Global.error !"Cleanup failed %{Error#mach}" err;
        return err'
      | Ok () -> continue ())
  in
  loop first_fcst_time
  >>= fun (res : Nothing.t Or_error.t) ->
  (* Nothing.t: loop will never exit successfully, it runs forever.
   * Perhaps its return type should simply be Error.t Deferred.t
   * Cons: don't get to use >>=? (convenient), don't get to use Nothing.t *)
  wait_for_mails ()
  >>= fun () ->
  Log.Global.flushed ()
  >>= fun () ->
  match res with
  | Ok _ -> assert false
  | Error err -> Error.raise err
;;

let shared_args () =
  let open Command.Spec in
  let log_level =
    Arg_type.create (fun s ->
        match String.lowercase s with
        | "info" -> `Info
        | "debug" -> `Debug
        | "error" -> `Error
        | _ -> failwithf "Invalid log level %s, choose info debug or error" s ())
  in
  empty
  ++ step (fun m directory -> m ?directory)
  +> flag
       "directory"
       (optional Filename.arg_type)
       ~doc:"DIR (optional) directory in which to place the dataset"
  ++ step (fun m log_level -> m ?log_level)
  +> flag "log-level" (optional log_level) ~doc:"DEBUG|INFO|ERROR (optional) log level"
;;

let forecast_time_arg =
  Command.Spec.Arg_type.create (fun s ->
      Or_error.ok_exn (Forecast_time.of_string_yyyymmddhh s))
;;

let one_cmd =
  Command.async_spec
    ~summary:"Download a specific dataset"
    Command.Spec.(shared_args () +> anon ("forecast_time" %: forecast_time_arg))
    one_main
;;

let daemon_cmd =
  Command.async_spec
    ~summary:"Start the downloader daemon"
    Command.Spec.(
      shared_args ()
      ++ step (fun m first_fcst_time -> m ?first_fcst_time)
      +> flag
           "first-forecast-time"
           (optional forecast_time_arg)
           ~doc:"YYYYMMDDHH first to download"
      ++ step (fun m error_rcpt_to -> m ~error_rcpt_to)
      +> flag
           "error-rcpt-to"
           (listed string)
           ~doc:"mail@domain send error notices to this address")
    daemon_main
;;

let cmd =
  Command.group
    ~summary:"Tawhiri dataset downloader"
    [ "one", one_cmd; "daemon", daemon_cmd ]
;;

let () = Command.run cmd

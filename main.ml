open Core.Std
open Async.Std
open Cohttp
open Cohttp_async
open Common

let one_main ?directory ?log_level forecast_time () =
  Option.iter log_level ~f:Log.Global.set_level;
  let wait_until = Forecast_time.expect_first_file_at forecast_time in
  begin
    if Time.(>) wait_until (Scheduler.cycle_start ())
    then Log.Global.info !"Waiting until %{Time} before starting %{Forecast_time}" wait_until forecast_time
  end;
  Clock.at wait_until >>= fun () ->
  let now = Scheduler.cycle_start () in
  let deadline =
    Time.max
      Forecast_time.(expect_first_file_at (incr forecast_time))
      (Time.add now (Time.Span.of_hr 2.))
  in
  Log.Global.info !"Deadline at %{Time} (in %{Time.Span})" deadline (Time.diff deadline now);
  let now = `no_longer_now in
  let `no_longer_now = now in
  let interrupt =
    let sigint = Ivar.create () in
    Signal.handle Signal.([int; term]) ~f:(Ivar.fill_if_empty sigint);
    choose
      [ choice (Clock.at deadline) (fun () -> "deadline")
      ; choice (Ivar.read sigint)  Signal.to_string
      ]
    >>= fun reason ->
    Log.Global.error "Interrupt: %s" reason;
    Deferred.unit
  in
  Download.download ~interrupt ?directory forecast_time
  >>= fun res ->
  Log.Global.flushed ()
  >>= fun () ->
  return (Or_error.ok_exn res)

let shared_args =
  let open Command.Spec in
  let log_level =
    Arg_type.create (fun s ->
      match String.lowercase s with
      | "info" -> `Info
      | "debug" -> `Debug
      | "error" -> `Error
      | _ -> failwithf "Invalid log level %s, choose info debug or error" s ()
    )
  in
  empty
  ++ step (fun m directory -> m ?directory)
  +> flag "directory" (optional file) ~doc:"DIR (optional) directory in which to place the dataset"
  ++ step (fun m log_level -> m ?log_level)
  +> flag "log-level" (optional log_level) ~doc:"DEBUG|INFO|ERROR (optional) log level"

let one_cmd = 
  Command.async
    ~summary:"Download a specific dataset"
    Command.Spec.(
      let forecast_time = Arg_type.create (fun s -> Or_error.ok_exn (Forecast_time.of_string_tawhiri s)) in
      shared_args
      +> anon ("forecast_time" %: forecast_time)
    )
    one_main

let cmd =
  Command.group
    ~summary:"Tawhiri dataset downloader"
    [ ("one", one_cmd) ]

let () = Command.run cmd

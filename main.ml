open Core.Std
open Async.Std
open Cohttp
open Cohttp_async
open Common

let download ~interrupt ~filename fcst_time =
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
    Download.get_message ~interrupt msg
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
    Download.get_index ~interrupt fcst_time level_set hour
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

let download_with_temp ~interrupt ?directory forecast_time =
  let open Deferred_result_infix in
  let (temp_filename, final_filename) =
    let open Dataset_file.Filename in
    ( one ?directory ~prefix:downloader_prefix forecast_time
    , one ?directory forecast_time
    )
  in
  Log.Global.debug "Temp filename will be %s" temp_filename;
  begin
    download ~interrupt ~filename:temp_filename forecast_time
    >>=?= fun () ->
    Log.Global.debug "Renaming %s -> %s" temp_filename final_filename;
    Monitor.try_with_or_error (fun () -> Sys.rename temp_filename final_filename)
  end
  >>= function
  | Ok () ->
    Log.Global.info "Completed successfully";
    return (Ok ())
  | Error _ as dl_err ->
    Monitor.try_with_or_error (fun () -> Sys.remove temp_filename)
    >>= fun delete_res ->
    begin
      match delete_res with
      | Ok () -> Log.Global.debug "Deleted %s" temp_filename
      | Error e -> Log.Global.debug !"Failed to delete temp file %s: %{Error#mach}" temp_filename e
    end;
    return dl_err

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
  download_with_temp ~interrupt ?directory forecast_time
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

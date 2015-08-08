open Core.Std
open Async.Std
open Cohttp
open Cohttp_async
open Common

let download_all ?directory ~interrupt fcst_time =
  let open Deferred_result_infix in
  Log.Global.debug !"Begin download of %{Forecast_time}" fcst_time;
  Dataset_file.create ?directory fcst_time RW
  >>=?= fun ds ->
  let maybe_wait =
    function
    | Some x -> x
    | None -> return (Ok ())
  in
  let job (hour, level_set) =
    let interrupt_this = Ivar.create () in
    upon interrupt (fun () -> Ivar.fill_if_empty interrupt_this ());
    let interrupt = Ivar.read interrupt_this in
    Download.get_index ~interrupt fcst_time level_set hour
    >>=?= fun messages ->
    let results =
      List.map messages ~f:(fun msg ->
        Download.get_message ~interrupt msg
        >>|?= fun grib ->
        let module G = Grib_index in
        let slice = Dataset_file.slice ds msg.G.hour msg.G.level msg.G.variable in
        Grib_message.blit grib slice
        >>-?| fun () ->
        Log.Global.debug !"Blitted %{Grib_index.message_to_string}" msg
      )
    in
    let error_early_warning =
      Deferred.create (fun ivar ->
        List.iter results ~f:(fun res ->
          res
          >>> function
          | Ok _ -> ()
          | Error e -> Ivar.fill_if_empty ivar e
        )
      )
    in
    choose
      [ choice (Deferred.all results) Or_error.combine_errors_unit
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
      let ongoing2 = Some (job hour_level_set) in
      loop ~ongoing1 ~ongoing2 ~waiting
  in
  let jobs =
    List.cartesian_product Hour.axis Level_set.([A; B])
    |> List.sort ~cmp:(fun (h1, _) (h2, _) -> Hour.compare h1 h2)
  in
  loop ~ongoing1:None ~ongoing2:None ~waiting:jobs

let main ?directory forecast_time ?log_level () =
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
  let interrupt = Clock.at deadline in
  download_all ?directory forecast_time ~interrupt
  >>| Or_error.ok_exn

let cmd = 
  Command.async
    ~summary:"Download a dataset"
    Command.Spec.(
      let forecast_time = Arg_type.create (fun s -> Or_error.ok_exn (Forecast_time.of_string_tawhiri s)) in
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
      +> anon ("forecast_time" %: forecast_time)
      ++ step (fun m log_level -> m ?log_level)
      +> flag "log-level" (optional log_level) ~doc:"DEBUG|INFO|ERROR (optional) log level"
    )
    main

let () = Command.run cmd

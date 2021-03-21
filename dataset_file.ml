open Core
open Async
open Common

type t = (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Genarray.t

module Filename = struct
  type 'a with_options = ?directory:string -> ?prefix:string -> ?suffix:string -> 'a

  let default_directory = "/srv/tawhiri-datasets"
  let downloader_prefix = "download-"

  let one ?(directory = default_directory) ?(prefix = "") ?(suffix = "") forecast_time =
    directory ^/ sprintf !"%s%{Forecast_time#yyyymmddhh}%s" prefix forecast_time suffix
  ;;

  type list_item =
    { fcst_time : Forecast_time.t
    ; basename : string
    ; path : string
    }

  let list ?(directory = default_directory) ?(prefix = "") ?(suffix = "") () =
    let len_ps = String.length prefix + String.length suffix in
    let sub s =
      String.sub s ~pos:(String.length prefix) ~len:(String.length s - len_ps)
    in
    match%bind Monitor.try_with_or_error (fun () -> Sys.ls_dir directory) with
    | Error _ as error -> return error
    | Ok basenames ->
      let basenames =
        List.filter_map basenames ~f:(fun basename ->
            let plausible =
              String.length basename > len_ps
              && String.is_prefix ~prefix basename
              && String.is_suffix ~suffix basename
            in
            if not plausible
            then None
            else (
              match Forecast_time.of_string_yyyymmddhh (sub basename) with
              | Ok fcst_time -> Some { fcst_time; basename; path = directory ^/ basename }
              | Error _ -> None))
      in
      return (Ok basenames)
  ;;
end

let shape = 65, 47, 3, 361, 720

let shape_arr =
  let a, b, c, d, e = shape in
  [| a; b; c; d; e |]
;;

let len_bytes = Array.fold shape_arr ~init:4 ~f:( * )

let () =
  let len = List.length in
  let a, b, c, _, _ = shape in
  assert (len Hour.axis = a);
  assert (len Level.axis = b);
  assert (len Variable.axis = c)
;;

let () =
  let open Ctypes in
  assert (sizeof (bigarray genarray shape_arr Bigarray.float32) = len_bytes)
;;

type mode =
  | RO
  | RW

let create ~filename mode =
  let module BA = Bigarray in
  let module Unix = Core.Unix in
  let unix_mode, shared =
    match mode with
    | RW -> [ Unix.O_RDWR; Unix.O_CREAT ], true
    | RO -> [ Unix.O_RDONLY ], false
  in
  Monitor.try_with_or_error (fun () ->
      In_thread.run (fun () ->
          Unix.with_file filename ~mode:unix_mode ~f:(fun fd ->
              Caml.Unix.map_file fd BA.float32 BA.c_layout shared shape_arr)))
;;

let slice t hour level variable =
  let slice =
    Bigarray.Genarray.slice_left
      t
      [| Hour.index hour; Level.index level; Variable.index variable |]
  in
  (* https://caml.inria.fr/mantis/view.php?id=7915 *)
  let mapped_ops = Obj.field (Obj.repr t) 0 in
  Obj.set_field (Obj.repr slice) 0 mapped_ops;
  Bigarray.array2_of_genarray slice
;;

let msync =
  let open Ctypes in
  let open Foreign in
  let f =
    foreign
      ~release_runtime_lock:true
      ~check_errno:true
      "msync"
      (ptr void @-> size_t @-> int @-> returning int)
  in
  let coerce = coerce (ptr float) (ptr void) in
  fun (t : t) ->
    let data = coerce (bigarray_start genarray t) in
    let ms_sync = 4 in
    let%bind result, wall_time_taken =
      In_thread.run ~name:"msync" (fun () ->
          let start = Time.now () in
          let result =
            Or_error.try_with (fun () ->
                f data (Unsigned.Size_t.of_int len_bytes) ms_sync)
          in
          let wall_time_taken = Time.diff (Time.now ()) start in
          result, wall_time_taken)
    in
    Log.Global.debug
      !"msync took %{Time.Span} and returned %{sexp:int Or_error.t}"
      wall_time_taken
      result;
    match result with
    | Ok 0 -> return (Ok ())
    | Ok x -> return (Or_error.errorf !"msync returned %i yet didn't set errno?" x)
    | Error _ as err -> return err
;;

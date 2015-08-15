open Core.Std
open Async.Std
open Common

type t = (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Genarray.t

module Filename = struct
  type 'a with_options = ?directory:string -> ?prefix:string -> ?suffix:string -> 'a

  let default_directory = "/srv/tawhiri-datasets"
  let downloader_prefix = "download-"

  let one ?(directory=default_directory) ?(prefix="") ?(suffix="") forecast_time =
    directory ^/ sprintf !"%s%{Forecast_time#tawhiri}%s" prefix forecast_time suffix

  type list_item =
    { fcst_time : Forecast_time.t
    ; basename : string
    ; path : string
    }

  let list ?(directory=default_directory) ?(prefix="") ?(suffix="") () =
    let open Deferred_result_infix in
    let len_ps = String.length prefix + String.length suffix in
    let sub s =
      String.sub s ~pos:(String.length prefix) ~len:(String.length s - len_ps)
    in 
    Monitor.try_with_or_error (fun () -> Sys.ls_dir directory)
    >>|?| List.filter_map ~f:(fun basename ->
      let plausible =
        String.length basename > len_ps
        && String.is_prefix ~prefix basename
        && String.is_suffix ~suffix basename
      in
      if not plausible
      then None
      else begin
        match Forecast_time.of_string_tawhiri (sub basename) with
        | Ok fcst_time -> Some { fcst_time; basename; path = directory ^/ basename }
        | Error _ -> None
      end
    )
end


let shape = (65, 47, 3, 361, 720)
let shape_arr = let (a, b, c, d, e) = shape in [|a;b;c;d;e|]

let () =
  let len = List.length in
  let (a, b, c, _, _) = shape in
  assert (len Hour.axis = a);
  assert (len Level.axis = b);
  assert (len Variable.axis = c)

type mode = RO | RW

let create ~filename mode =
  let module BA = Bigarray in
  let module Unix = Core.Std.Unix in
  let (unix_mode, shared) =
    match mode with
    | RW -> ([Unix.O_RDWR; Unix.O_CREAT], true)
    | RO -> ([Unix.O_RDONLY], false)
  in
  Monitor.try_with_or_error (fun () ->
    In_thread.run (fun () ->
      Unix.with_file filename ~mode:unix_mode ~f:(fun fd ->
        BA.Genarray.map_file fd BA.float32 BA.c_layout shared shape_arr
      )
    )
  )

let slice t hour level variable =
  Bigarray.Genarray.slice_left t [|Hour.index hour; Level.index level; Variable.index variable|]
  |> Bigarray.array2_of_genarray

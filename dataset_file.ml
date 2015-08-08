open Core.Std
open Async.Std
open Common

type t = (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Genarray.t

let filename ?(directory="/srv/tawhiri-datasets") forecast_time =
  directory ^/ Forecast_time.to_string_tawhiri forecast_time

let shape = (65, 47, 3, 361, 720)
let shape_arr = let (a, b, c, d, e) = shape in [|a;b;c;d;e|]

let () =
  let len = List.length in
  let (a, b, c, _, _) = shape in
  assert (len Hour.axis = a);
  assert (len Level.axis = b);
  assert (len Variable.axis = c)

type mode = RO | RW

let create ?directory fcst_time mode =
  let module BA = Bigarray in
  let module Unix = Core.Std.Unix in
  let (unix_mode, shared) =
    match mode with
    | RW -> ([Unix.O_RDWR; Unix.O_CREAT], true)
    | RO -> ([Unix.O_RDONLY], false)
  in
  Monitor.try_with_or_error (fun () ->
    In_thread.run (fun () ->
      Unix.with_file (filename ?directory fcst_time) ~mode:unix_mode ~f:(fun fd ->
        BA.Genarray.map_file fd BA.float32 BA.c_layout shared shape_arr
      )
    )
  )

let slice t hour level variable =
  Bigarray.Genarray.slice_left t [|Hour.index hour; Level.index level; Variable.index variable|]
  |> Bigarray.array2_of_genarray

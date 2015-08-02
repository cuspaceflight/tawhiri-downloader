open Core.Std
open Async.Std
open Common

module F : sig
  type grib_handle_and_data
  val grib_handle_new_from_message : Bigstring.t -> grib_handle_and_data Or_error.t
  val grib_get_long : grib_handle_and_data -> string -> Int64.t Or_error.t
  val grib_get_int : grib_handle_and_data -> string -> int Or_error.t
  val grib_get_double : grib_handle_and_data -> string -> float Or_error.t
end = struct
  open Ctypes
  open Foreign
  module Gc = Core.Std.Gc

  type grib_handle_struct
  let grib_handle_struct : grib_handle_struct structure typ = structure "grib_handle"
  type grib_handle = grib_handle_struct structure ptr
  let grib_handle = ptr grib_handle_struct
  let grib_handle_or_null = ptr_opt grib_handle_struct

  type grib_handle_and_data = grib_handle * Bigstring.t

  let grib_get_error_message =
    let f =
      foreign "grib_get_error_message" (int @-> returning string_opt)
    in
    fun c ->
      match f c with
      | None -> sprintf "unknown grib error %i" c
      | Some s -> s

  let grib_check =
    function
    | 0 -> Ok ()
    | error_code -> Error (Error.of_string (grib_get_error_message error_code))

  let grib_check_exn x = Or_error.ok_exn (grib_check x)

  let grib_handle_delete =
    foreign
      "grib_handle_delete"
      (grib_handle @-> returning int)

  let grib_handle_new_from_message =
    let f = 
      foreign
        "grib_handle_new_from_message"
        (ptr_opt void @-> ptr char @-> size_t @-> returning grib_handle_or_null)
    in
    let finaliser (handle, _bs) =
      match grib_handle_delete handle with
      | 0 -> ()
      | e -> Log.Global.error "grib_handle_new_from_message error finalising %s" (grib_get_error_message e)
    in
    fun bigstr ->
      let data = array_of_bigarray array1 bigstr in
      match f None (CArray.start data) (Unsigned.Size_t.of_int (CArray.length data)) with
      | None -> Error (Error.of_string "grib_handle_new_from_message")
      | Some handle ->
        let t = (handle, bigstr) in
        Gc.minor ();
        Gc.Expert.add_finalizer_exn t finaliser;
        Ok t

  let grib_get_long =
    let f =
      foreign
        "grib_get_long"
        (grib_handle @-> string @-> ptr long @-> returning int)
    in
    fun (handle, _) key ->
      let open Result.Monad_infix in
      let temp = allocate long (Signed.Long.of_int 0) in
      grib_check (f handle key temp) >>| fun () ->
      Signed.Long.to_int64 (!@ temp)

  let grib_get_int t key =
    let open Result.Monad_infix in
    grib_get_long t key >>= fun res ->
    match Int64.to_int res with
    | Some x -> Ok x
    | None -> Error (Error.create "variable Int64.to_int" (key, res) <:sexp_of< string * Int64.t >>)

  let grib_get_double =
    let f =
      foreign
        "grib_get_double"
        (grib_handle @-> string @-> ptr double @-> returning int)
    in
    fun (handle, _) key ->
      let open Result.Monad_infix in
      let temp = allocate double 0. in
      grib_check (f handle key temp) >>| fun () ->
      !@ temp
end

type t = F.grib_handle_and_data

let of_bigstring = F.grib_handle_new_from_message

let variable t =
  let open Result.Monad_infix in
  let g = F.grib_get_int t in
  g "discipline" >>= fun a ->
  g "parameterCategory" >>= fun b ->
  g "parameterNumber" >>= fun c ->
  match (a, b, c) with
  | (0, 3, 5) -> Ok Variable.Height
  | (0, 2, 2) -> Ok Variable.U_wind
  | (0, 2, 3) -> Ok Variable.V_wind
  | (a, b, c) -> Error (Error.of_string (sprintf "couldn't identify variable %i %i %i" a b c))

let layout t =
  let open Result.Monad_infix in
  let gi = F.grib_get_int t in
  let gd = F.grib_get_double t in
  gi "iScansNegatively" >>= fun a ->
  gi "jScansPositively" >>= fun b ->
  gi "Ni" >>= fun c ->
  gi "Nj" >>= fun d ->
  gd "latitudeOfFirstGridPointInDegrees" >>= fun e ->
  gd "longitudeOfFirstGridPointInDegrees" >>= fun f ->
  gd "latitudeOfLastGridPointInDegrees" >>= fun g ->
  gd "longitudeOfLastGridPointInDegrees" >>= fun h ->
  gd "iDirectionIncrementInDegrees" >>= fun i ->
  gd "jDirectionIncrementInDegrees" >>= fun j ->
  gi "numberOfValues" >>= fun k ->
  match (a, b, c, d, e, f, g, h, i, j, k) with
  | (0, 0, 720, 361, 90., 0., -90., 359.5, 0.5, 0.5, 259920) ->
    Ok Layout.Half_deg
  | (a, b, c, d, e, f, g, h, i, j, k) ->
    Error (
      Error.of_string (
        sprintf
          "couldn't identify layout %i %i %i %i %f %f %f %f %f %f %i"
          a b c d e f g h i j k
      )
    )

let hour t =
  let open Result.Monad_infix in
  let gi = F.grib_get_int t in
  gi "stepUnits" >>= fun a ->
  gi "forecastTime" >>= fun b ->
  match (a, b) with
  | (1, n) when 0 <= n && n <= 192 && n mod 3 = 0 -> Ok n
  | (a, b) -> Error (Error.of_string (sprintf "couldn't identify hour %i %i" a b))

let level t =
  let open Result.Monad_infix in
  let gi = F.grib_get_int t in
  gi "typeOfFirstFixedSurface" >>= fun a ->
  gi "scaleFactorOfFirstFixedSurface" >>= fun b ->
  gi "scaledValueOfFirstFixedSurface" >>= fun c ->
  gi "level" >>= fun d ->
  match (a, b, c, d) with
  | (100, 0, n, m) when n = m * 100 ->
    Ok (Level.Mb m)
  | (a, b, c, d) ->
    Error (Error.of_string (sprintf "couldn't identify level %i %i %i %i" a b c d))

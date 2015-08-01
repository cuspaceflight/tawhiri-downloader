open Core.Std
open Async.Std

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
    foreign "grib_get_error_message" (int @-> returning string)

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
      | 0 -> Log.Global.info "grib_handle_new_from_message finalised"
      | e -> Log.Global.error "grib_handle_new_from_message error finalising %i" e
    in
    fun bigstr ->
      let data = array_of_bigarray array1 bigstr in
      match f None (CArray.start data) (Unsigned.Size_t.of_int (CArray.length data)) with
      | None -> Error (Error.of_string "grib_handle_new_from_message")
      | Some handle ->
        Log.Global.info "grib_handle_new_from_message attaching finaliser";
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

let of_data_pipe ~total_len p =
  Log.Global.info "Reading data pipe into buffer len %i" total_len;
  let bigstring = Bigstring.create total_len in
  let iobuf = Iobuf.of_bigstring bigstring in
  let rec load_loop () =
    Pipe.read p >>= fun src ->
    match src with
    | `Eof ->
      if Iobuf.is_empty iobuf
      then return (Ok ())
      else return (Error (Error.of_string "unexpected EOF"))
    | `Ok src ->
      if Iobuf.length iobuf < String.length src
      then return (Error (Error.of_string "too much data in pipe"))
      else begin
        Iobuf.Fill.string iobuf src;
        Log.Global.info "Got %i" (total_len - Iobuf.length iobuf);
        load_loop ()
      end
  in
  load_loop () >>=? fun () ->
  return (of_bigstring bigstring)

let variable t =
  let open Result.Monad_infix in
  let g = F.grib_get_int t in
  g "discipline" >>= fun a ->
  g "parameterCategory" >>= fun b ->
  g "parameterNumber" >>= fun c ->
  match (a, b, c) with
  | (0, 3, 5) -> Ok `height
  | (0, 2, 2) -> Ok `u_wind
  | (0, 2, 3) -> Ok `v_wind
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
  | (0, 0, 720, 361, 90., 0., -90., 359.5, 0.5, 0.5, 259920) -> Ok `half_deg
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
  | (100, 0, n, m) when n = m * 100 -> Ok m
  | (a, b, c, d) ->
    Error (Error.of_string (sprintf "couldn't identify level %i %i %i %i" a b c d))

let test =
  let (url, offset, length) =
    ( "http://ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.2015080106/gfs.t06z.pgrb2b.0p50.f159"
    , 6364853
    , 6496803 - 6364853
    )
  in
  let open Cohttp in
  let open Cohttp_async in
  let range_header = sprintf "bytes=%i-%i" offset (offset + length - 1) in
  Log.Global.info !"Sending request %s (expecting %i bytes)" range_header length;
  Client.get (Uri.of_string url) ~headers:(Header.init_with "range" range_header)
  >>= fun (resp, body) ->
  Log.Global.info !"Response %{sexp:Response.t}" resp;
  begin
    match Response.status resp with
    | #Code.success_status ->
      return (Ok ())
    | other ->
      return (Error (Error.create "HTTP response" other Code.sexp_of_status_code))
  end
  >>=? fun () ->
  of_data_pipe ~total_len:length (Body.to_pipe body)
  >>=? fun handle ->
  return (
    let open Result.Monad_infix in
    variable handle >>= fun variable ->
    layout handle >>= fun `half_deg ->
    level handle >>= fun level ->
    hour handle >>| fun hour ->
    (variable, hour, level)
  )

let _ = Thread_safe.block_on_async_exn (fun () ->
  test 
  >>| Or_error.ok_exn
  >>= fun (variable, hour, level) ->
  Log.Global.info !"got %{Sexp} %i %i" (<:sexp_of< [`height|`u_wind|`v_wind] >> variable) hour level;
  Log.Global.flushed ()
)

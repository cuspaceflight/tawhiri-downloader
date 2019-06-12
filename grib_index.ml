open Core
open Common

type line = 
  { idx : int
  ; offset : int
  ; fcst_time : Forecast_time.t
  ; variable : Variable.t
  ; level : Level.t
  ; hour : Hour.t
  }

type message =
  { offset : int
  ; length : int
  ; fcst_time : Forecast_time.t
  ; variable : Variable.t
  ; level : Level.t
  ; hour : Hour.t
  }

let message_to_string { offset; length; fcst_time; variable; level; hour } =
  sprintf
    !"%{Forecast_time#yyyymmddhh} %{Variable} %{Level} %{Hour} (%i;%i)"
    fcst_time variable level hour offset length

let drop_suffix ~suffix s =
  if String.is_suffix ~suffix s
  then Ok (String.subo ~len:(String.length s - String.length suffix) s)
  else Or_error.errorf "expected suffix %s in %s" suffix s

let int_of_string s = Or_error.try_with (fun () -> Int.of_string s)

let parse_variable =
  function
  | "HGT" -> Ok Variable.Height
  | "UGRD" -> Ok Variable.U_wind
  | "VGRD" -> Ok Variable.V_wind
  | other -> Or_error.errorf "couldn't identify variable %s" other

let parse_hour =
  let open Result.Monad_infix in
  function
  | "anl" -> Hour.of_int 0
  | hour ->
    drop_suffix hour ~suffix:" hour fcst"
    >>= int_of_string
    >>= Hour.of_int

let parse_fcst_time s =
  if not (String.is_prefix s ~prefix:"d=")
  then Or_error.error_string "fcst time prefix"
  else Forecast_time.of_string_yyyymmddhh (String.subo s ~pos:2)

let parse_level s =
  let open Result.Monad_infix in
  drop_suffix ~suffix:" mb" s
  >>= int_of_string
  >>= Level.of_mb

(* 15:1207405:d=2015080106:CLWMR:2 mb:159 hour fcst: *)
let parse_line =
  let open Result.Monad_infix in
  function
  | [idx; offset; fcst_time; variable; level; hour; ""] ->
    int_of_string offset >>= fun offset ->
    int_of_string idx >>= fun idx ->
    parse_fcst_time fcst_time >>= fun fcst_time ->
    parse_variable variable >>= fun variable ->
    parse_level level >>= fun level ->
    parse_hour hour >>= fun hour ->
    Ok { idx; offset; fcst_time; variable; level; hour }
  | _ -> Or_error.error_string "malformed line"

let parse_idx_offset =
  let open Result.Monad_infix in
  function
  | idx::offset::_ ->
    int_of_string idx >>= fun idx ->
    int_of_string offset >>= fun offset ->
    Ok (idx, offset)
  | _ -> Or_error.error_string "malformed line"

let parse index =
  let open Result.Monad_infix in
  let rec loop parsed_lines input_lines =
    match input_lines with
    | x::(y::_ as xs) ->
      begin
        match parse_line x with
        | Ok { idx; offset; fcst_time; variable; level; hour } ->
          parse_idx_offset y >>= fun (next_idx, next_offset) ->
          let length = next_offset - offset in
          if next_idx = idx + 1 && length > 0
          then loop ({ offset; length; fcst_time; variable; level; hour } :: parsed_lines) xs
          else Or_error.errorf "line after %i made no sense" idx
        | Error _ ->
          (* didn't recognise this line; don't care. *)
          loop parsed_lines xs
      end
    | [x] ->
      begin
        match parse_line x with
        | Ok _ -> Or_error.error_string "not implemented: line we care about at end of file"
        | Error _ -> Ok parsed_lines
      end
    | [] -> Ok parsed_lines
  in
  String.split_lines index
  |> List.map ~f:(String.split ~on:':')
  |> loop []
  |> Or_error.map ~f:List.rev

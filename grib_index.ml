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
    fcst_time
    variable
    level
    hour
    offset
    length
;;

let parse_variable = function
  | "HGT" -> Ok Variable.Height
  | "UGRD" -> Ok Variable.U_wind
  | "VGRD" -> Ok Variable.V_wind
  | other -> Or_error.errorf "couldn't identify variable %s" other
;;

let parse_hour =
  let extract_int_exn = function
    | "anl" -> 0
    | hour -> hour |> String.chop_suffix_exn ~suffix:" hour fcst" |> Int.of_string
  in
  fun hour ->
    match extract_int_exn hour with
    | exception _ -> Or_error.errorf "illegal hour string: %s" hour
    | hour -> Hour.of_int hour
;;

let parse_fcst_time string =
  match String.chop_prefix string ~prefix:"d=" with
  | None -> Or_error.error_string "fcst time prefix"
  | Some string -> Forecast_time.of_string_yyyymmddhh string
;;

let parse_level =
  let extract_int_exn string =
    string |> String.chop_suffix_exn ~suffix:" mb" |> Int.of_string
  in
  fun level ->
    match extract_int_exn level with
    | exception _ -> Or_error.errorf "illegal level string: %s" level
    | level -> Level.of_mb level
;;

let try_int_of_string str =
  match Int.of_string str with
  | exception _ -> Or_error.errorf "not an int: %s" str
  | int -> Ok int
;;

(* 15:1207405:d=2015080106:CLWMR:2 mb:159 hour fcst: *)
let parse_line = function
  | [ idx; offset; fcst_time; variable; level; hour; "" ] ->
    let%bind.Result offset = try_int_of_string offset
    and idx = try_int_of_string idx
    and fcst_time = parse_fcst_time fcst_time
    and variable = parse_variable variable
    and level = parse_level level
    and hour = parse_hour hour in
    Ok { idx; offset; fcst_time; variable; level; hour }
  | _ -> Or_error.error_string "malformed line"
;;

let parse_idx_offset = function
  | idx :: offset :: _ ->
    let%bind.Result offset = try_int_of_string offset
    and idx = try_int_of_string idx in
    Ok (idx, offset)
  | _ -> Or_error.error_string "malformed line"
;;

let parse index =
  let rec loop ~parsed_lines ~input_lines =
    match input_lines with
    | [] -> Ok (List.rev parsed_lines)
    | next_line :: input_lines_tl ->
      (match parse_line next_line with
      | Error _ ->
        (* didn't recognise this line; don't care. *)
        loop ~parsed_lines ~input_lines:input_lines_tl
      | Ok { idx; offset; fcst_time; variable; level; hour } ->
        let peek_line_after =
          match input_lines_tl with
          | l :: _ -> parse_idx_offset l
          | [] ->
            Or_error.error_string "not implemented: line we care about at end of file"
        in
        (match peek_line_after with
        | Error _ as error -> error
        | Ok (next_idx, next_offset) ->
          let length = next_offset - offset in
          let parsed_lines =
            { offset; length; fcst_time; variable; level; hour } :: parsed_lines
          in
          if next_idx = idx + 1 && length > 0
          then loop ~parsed_lines ~input_lines:input_lines_tl
          else Or_error.errorf "line after %i made no sense" idx))
  in
  let input_lines = String.split_lines index |> List.map ~f:(String.split ~on:':') in
  loop ~parsed_lines:[] ~input_lines
;;

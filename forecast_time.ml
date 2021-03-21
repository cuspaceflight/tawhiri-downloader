open Core

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ] [@@deriving sexp, compare, equal]

let incr =
  function
  | (d, `h00) -> (d, `h06)
  | (d, `h06) -> (d, `h12)
  | (d, `h12) -> (d, `h18)
  | (d, `h18) -> (Date.add_days d 1, `h00)

let hour_int' = 
  function
  | `h00 -> 0
  | `h06 -> 6
  | `h12 -> 12
  | `h18 -> 18

let hour_int (_, h) = hour_int' h

let to_string_gen fmt (date, hour) =
  let hour = hour_int' hour in
  let year = Date.year date in
  let month = Month.to_int (Date.month date) in
  let day = Date.day date in
  sprintf fmt year month day hour

let to_string_yyyymmddhh = to_string_gen !"%04i%02i%02i%02i"
let to_string_yyyymmdd_slash_hh = to_string_gen !"%04i%02i%02i/%02i"

let () = 
  [%test_eq: string] (to_string_yyyymmddhh        (Date.of_string "1994-03-14", `h06)) "1994031406";
  [%test_eq: string] (to_string_yyyymmdd_slash_hh (Date.of_string "1994-03-14", `h06)) "19940314/06"

let of_string_yyyymmddhh s =
  let err s = Or_error.errorf "Bad Forecast time string (yyyymmddhh) %s" s in
  if String.length s <> 10
  then err s
  else
    try
      let date = Date.of_string (String.sub s ~pos:0 ~len:8) in
      let hour =
        match String.sub s ~pos:8 ~len:2 with
        | "00" -> `h00
        | "06" -> `h06
        | "12" -> `h12
        | "18" -> `h18
        | _ -> failwith "hour"
      in  
      Ok (date, hour)
    with 
    | _ -> err s

let () =
  [%test_eq: t Or_error.t]
    (of_string_yyyymmddhh "1994031418") 
    (Ok (Date.of_string "1994-03-14", `h18))

(* The first file appears at about +3h30, and it's all up by about +4h30 *)
let expect_first_file_at (date, hour) =
  let ofday = Time.Ofday.create ~hr:(hour_int' hour) () in
  let ds_time = Time.of_date_ofday date ofday ~zone:Time.Zone.utc in
  Time.add ds_time (Time.Span.of_hr 3.5)

let expect_next_release () =
  let ds_time = Time.(sub (now ()) (Span.of_hr 4.5)) in
  let (date, ofday) = Time.to_date_ofday ds_time ~zone:Time.Zone.utc in
  let hour =
    Time.Ofday.to_parts ofday
    |> (fun x -> x.Time.Span.Parts.hr)
    |> (fun x -> x - (x mod 6))
  in
  let hour =
    match hour with
    | 0  -> `h00
    | 6  -> `h06
    | 12 -> `h12
    | 18 -> `h18
    | hour -> failwithf "bad hour after rounding: %i" hour ()
  in
  (date, hour)

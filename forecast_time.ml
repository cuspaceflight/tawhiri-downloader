open Core.Std

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ] with sexp

let incr =
  function
  | (d, `h00) -> (d, `h06)
  | (d, `h06) -> (d, `h12)
  | (d, `h12) -> (d, `h18)
  | (d, `h18) -> (Date.add_days d 1, `h00)

let to_string (date, hour) =
  let hour =
    match hour with
    | `h00 -> 0
    | `h06 -> 6
    | `h12 -> 12
    | `h18 -> 18
  in
  sprintf !"%{Date} %02i" date hour

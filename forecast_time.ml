open Core.Std

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ]

let incr =
  function
  | (d, `h00) -> (d, `h06)
  | (d, `h06) -> (d, `h12)
  | (d, `h12) -> (d, `h18)
  | (d, `h18) -> (Date.add_days d 1, `h00)

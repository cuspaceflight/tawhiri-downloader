open Core.Std

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ]

val to_string : t -> string

val incr : t -> t

(* val expect_at : t -> Time.t
val deadline  : t -> Time.t *)

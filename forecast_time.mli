open Core.Std

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ]

val incr : t -> t

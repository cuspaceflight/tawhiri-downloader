open Core

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ]

val hour_int : t -> int
val to_string_yyyymmddhh : t -> string
val to_string_yyyymmdd_slash_hh : t -> string

val of_string_yyyymmddhh : string -> t Or_error.t

val incr : t -> t

val expect_first_file_at : t -> Time.t
val expect_next_release : unit -> t

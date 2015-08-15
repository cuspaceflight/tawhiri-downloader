open Core.Std

type t = Date.t * [ `h00 | `h06 | `h12 | `h18 ]

val hour_int : t -> int
val to_string : t -> string
val to_string_noaa : t -> string
val to_string_tawhiri : t -> string

val of_string_tawhiri : string -> t Or_error.t
val of_string_noaa : string -> t Or_error.t

val incr : t -> t

val expect_first_file_at : t -> Time.t
val expect_next_release : unit -> t

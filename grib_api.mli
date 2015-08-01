open Core.Std
open Async.Std

type t

val of_bigstring : Bigstring.t -> t Or_error.t

val variable : t -> [ `height | `u_wind | `v_wind ] Or_error.t
val hour : t -> int Or_error.t
val level : t -> int Or_error.t
val layout : t -> [ `half_deg ] Or_error.t

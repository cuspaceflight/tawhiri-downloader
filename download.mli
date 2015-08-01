open Core.Std
open Async.Std

val get
  :  Uri.t
  -> timeout:Time.Span.t
  -> progress:(int -> unit)
  -> range:[ `exactly_pos_len of int * int | `all_with_max_len of int ]
  -> Bigstring.t Or_error.t Deferred.t

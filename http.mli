open Core.Std
open Async.Std

val get
  :  Uri.t
  -> interrupt:unit Deferred.t
  -> range:[ `exactly_pos_len of int * int | `all_with_max_len of int ]
  -> Bigstring.t Or_error.t Deferred.t

open Core.Std
open Async.Std

type range = [ `exactly_pos_len of int * int | `all_with_max_len of int ] with sexp

val get
  :  Uri.t
  -> interrupt:unit Deferred.t
  -> range:range
  -> Bigstring.t Or_error.t Deferred.t

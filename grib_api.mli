open Core.Std
open Async.Std

type t
val of_bigstring : Bigstring.t -> t Or_error.t
val of_data_pipe : total_len:int -> string Pipe.Reader.t -> t Or_error.t Deferred.t

open Core.Std
open Async.Std
open Common

type t

val of_bigstring : Bigstring.t -> t Or_error.t

val variable : t -> Variable.t Or_error.t
val hour : t -> int Or_error.t
val level : t -> Level.t Or_error.t
val layout : t -> Layout.t Or_error.t

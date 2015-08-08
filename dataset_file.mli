open Core.Std
open Async.Std
open Common

type t

val filename : ?directory:string -> Forecast_time.t -> string
val shape : int * int * int * int * int
val shape_arr : int array

type mode = RO | RW

val create : ?directory:string -> Forecast_time.t -> mode -> t Or_error.t Deferred.t

val slice
  :  t
  -> Hour.t
  -> Level.t
  -> Variable.t
  -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array2.t

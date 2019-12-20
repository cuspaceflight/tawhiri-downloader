open Core
open Async
open Common

type t

module Filename : sig
  type 'a with_options = ?directory:string -> ?prefix:string -> ?suffix:string -> 'a
  val default_directory : string
  val downloader_prefix : string
  val one : (Forecast_time.t -> string) with_options

  type list_item =
    { fcst_time : Forecast_time.t
    ; basename : string
    ; path : string
    }
  val list : (unit -> list_item list Or_error.t Deferred.t) with_options
end

type mode = RO | RW

val create : filename:string -> mode -> t Or_error.t Deferred.t

val slice
  :  t
  -> Hour.t
  -> Level.t
  -> Variable.t
  -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array2.t

val msync : t -> unit Or_error.t Deferred.t

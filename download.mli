open Core
open Async
open Common

module Base_url : sig
  type t =
    | Nomads
    | Aws_mirror
end

val get_index
  :  interrupt:unit Deferred.t
  -> Base_url.t
  -> Forecast_time.t
  -> Level_set.t
  -> Hour.t
  -> Grib_index.message list Or_error.t Deferred.t

val get_message
  :  interrupt:unit Deferred.t
  -> Base_url.t
  -> Grib_index.message
  -> Grib_message.t Or_error.t Deferred.t

val download
  :  interrupt:unit Deferred.t
  -> ?directory:string
  -> Base_url.t
  -> Forecast_time.t
  -> unit Or_error.t Deferred.t

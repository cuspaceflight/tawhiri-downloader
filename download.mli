open Core.Std
open Async.Std
open Common

val get_index
  :  interrupt:unit Deferred.t 
  -> Forecast_time.t
  -> Level_set.t
  -> Hour.t
  -> Grib_index.message list Or_error.t Deferred.t

val get_message
  :  interrupt:unit Deferred.t
  -> Grib_index.message
  -> Grib_message.t Or_error.t Deferred.t

val download
  :  interrupt:unit Deferred.t
  -> ?directory:string
  -> Forecast_time.t
  -> unit Or_error.t Deferred.t

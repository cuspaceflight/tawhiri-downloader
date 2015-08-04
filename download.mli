open Core.Std
open Async.Std
open Common

val get_index
  :  interrupt:unit Deferred.t 
  -> Forecast_time.t
  -> [ `pgrb2 | `pgrb2b ]
  -> hour:int
  -> Grib_index.message list Or_error.t Deferred.t

val get_message
  :  interrupt:unit Deferred.t
  -> Grib_index.message
  -> Grib_message.t Or_error.t Deferred.t

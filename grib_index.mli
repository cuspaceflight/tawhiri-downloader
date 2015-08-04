open Core.Std
open Common

type message =
  { offset : int 
  ; length : int 
  ; fcst_time : Forecast_time.t
  ; variable : Variable.t
  ; level : Level.t
  ; hour : int 
  }

val parse : string -> message list Or_error.t

val string_of_message : message -> string

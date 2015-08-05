open Core.Std
open Common

type t

val of_bigstring : Bigstring.t -> t Or_error.t

val variable : t -> Variable.t Or_error.t
val hour : t -> int Or_error.t
val level : t -> Level.t Or_error.t
val layout : t -> Layout.t Or_error.t
(* blit will fix up the data so that it is C layout [latitude][longitude],
 * both axes increasing. *)
val blit 
  :  t 
  -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
  -> dst_pos:int 
  -> unit Or_error.t

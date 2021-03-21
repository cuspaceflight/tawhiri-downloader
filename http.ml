open Core
open Async

type range = [ `exactly_pos_len of int * int | `all_with_max_len of int ] [@@deriving sexp]

(* XXX: switch to ocurl. *)
let get _uri ~interrupt:_ ~range:_ = ignore return; assert false

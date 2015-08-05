open Core.Std

module Variable = struct
  type t =
    | U_wind
    | V_wind
    | Height
  with sexp, compare

  let to_string =
    function
    | U_wind -> "U_wind"
    | V_wind -> "V_wind"
    | Height -> "Height"
end

module Level = struct
  type t =
    | Mb of int
  with sexp, compare

  let to_string (Mb x) = sprintf "%i mb" x
end

module Layout = struct
  type t =
    | Half_deg
end

module Deferred_result_infix = struct
  open Async.Std

  let (>>|?) = `no 
  let (>>=?) = `no 

  (* please forgive me. *)
  let (>>=?=) x f = 
    x >>= function
    | Ok y -> f y 
    | (Error _ as e) -> return e
  let (>>|?=) x f = 
    x >>| fun x ->
    Result.bind x f 
  let (>>=?|) x f = 
    x >>= function
    | Ok x ->
      f x >>| fun x ->
      Ok x
    | (Error _ as e) -> return e
  let (>>|?|) x f = 
    x >>| fun x ->
    Result.map x ~f

  let (>>-?=) = Result.bind
  let (>>-?|) x f = Result.map x ~f
end

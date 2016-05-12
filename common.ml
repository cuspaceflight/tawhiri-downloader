open Core.Std

module Hour : sig
  type t = private int [@@deriving sexp, compare]
  val of_int : int -> t Or_error.t
  val to_string : t -> string
  val to_int : t -> int
  val axis : t list
  val index : t -> int
end = struct
  type t = int [@@deriving sexp, compare]

  let of_int i =
    if i mod 3 = 0 && 0 <= i && i <= 192
    then Ok i
    else Or_error.errorf "Invalid hour %i" i

  let to_string = sprintf "%i hrs" 

  let to_int i = i

  let axis = List.range ~stride:3 ~start:`inclusive ~stop:`inclusive 0 192

  let index i = i / 3

  let () = List.iter axis ~f:(fun h -> assert (of_int h = Ok h))
  let () = List.iteri axis ~f:(fun idx hour -> assert (index hour = idx))
end

module Variable = struct
  type t =
    | U_wind
    | V_wind
    | Height
  [@@deriving sexp, compare]

  let to_string =
    function
    | U_wind -> "U_wind"
    | V_wind -> "V_wind"
    | Height -> "Height"

  let index =
    function
    | Height -> 0
    | U_wind -> 1
    | V_wind -> 2

  let axis = [Height; U_wind; V_wind]

  let () = List.iteri axis ~f:(fun i x -> assert (index x = i))
end

module Level_set = struct
  type t =
    | A  (* pgrb2 *)
    | B  (* pgrb2b *)
  [@@deriving sexp, compare]

  let to_string =
    function
    | A -> "pgrb"
    | B -> "pgrb2b"
end

module Level : sig
  type t [@@deriving sexp, compare]

  val of_mb : int -> t Or_error.t
  val to_mb : t -> int
  val axis : t list
  val index : t -> int
  val to_string : t -> string

  (** For the [t]s that appear in both level sets, we've arbitrarily assigned
      them to exactly one; [ts_in A] and [ts_in B] partition the levels and
      you're guaranteed that [List.mem (ts_in (level_set t)) t] and 
      [List.forall (ts_in ls) ~f:(fun t -> level_set t = ls). *)
  val ts_in : Level_set.t -> t list
  val level_set : t -> Level_set.t
end = struct
  type t = int [@@deriving sexp]

  let compare = Fn.flip Int.compare (* higher pressures first *)
  let to_string = sprintf "%i mb"
  let to_mb x = x

  (* 1, 2, 3, 5, 7 appear in both files. We ignore the copies in the A set. *)
  let mbs_pgrb2 = 
    [ 10; 20; 30; 50; 70; 100; 150; 200; 250; 300; 350; 400 
    ; 450; 500; 550; 600; 650; 700; 750; 800; 850; 900; 925 
    ; 950; 975; 1000
    ]

  let mbs_pgrb2b =
    [ 1; 2; 3; 5; 7; 125; 175; 225; 275; 325; 375; 425 
    ; 475; 525; 575; 625; 675; 725; 775; 825; 875 
    ]

  let of_mb =
    let hst = Int.Hash_set.create () in
    List.iter (mbs_pgrb2 @ mbs_pgrb2b) ~f:(Hash_set.add hst);
    fun mb ->
      if Hash_set.mem hst mb
      then Ok (mb : t)
      else Or_error.errorf "not a level: %i mb" mb

  let level_set =
    let f mbs ls = List.map mbs ~f:(fun mb -> (mb, ls)) in
    (f mbs_pgrb2 Level_set.A @ f mbs_pgrb2b Level_set.B)
    |> Int.Table.of_alist_exn
    |> Hashtbl.find_exn

  let axis = List.sort ~cmp:compare (mbs_pgrb2 @ mbs_pgrb2b)

  let ts_in s =
    match (s : Level_set.t) with
    | A -> mbs_pgrb2
    | B -> mbs_pgrb2b

  let index =
    List.mapi axis ~f:(fun i mb -> (mb, i))
    |> Int.Table.of_alist_exn
    |> Hashtbl.find_exn

  let () = List.iter mbs_pgrb2  ~f:(fun x -> assert (level_set x = Level_set.A))
  let () = List.iter mbs_pgrb2b ~f:(fun x -> assert (level_set x = Level_set.B))
  let () = List.iter axis ~f:(fun x -> assert (of_mb x = Ok x))
  let () = List.iteri axis ~f:(fun idx mb -> assert (index mb = idx))
  let () = assert (List.hd_exn axis = 1000)
  let () = assert (List.last_exn axis = 1)
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

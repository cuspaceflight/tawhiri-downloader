open Core.Std
open Async.Std
open Cohttp
open Cohttp_async

(* WARNING! Until https://github.com/mirage/ocaml-cohttp/issues/444 and
 * https://github.com/mirage/ocaml-cohttp/issues/445 are fixed upstream, you
 * need a patched cohttp to avoid leaking file descriptors. See cohttp-patch.diff *)

type range = [ `exactly_pos_len of int * int | `all_with_max_len of int ] with sexp

(* I'd rather use Iobufs everywhere, but other bits need to take bigstrings.
 * The iobuf here is just a temporary one looking at the same backing bigstring
 * for convenience. *)
let drain_pipe_to_bigstring ~max_len pipe =
  let bigstring = Bigstring.create max_len in
  let iobuf = Iobuf.of_bigstring bigstring in
  let rec load_loop () =
    Pipe.read pipe >>= fun src ->
    match src with
    | `Eof ->
      Iobuf.flip_lo iobuf;
      return (Ok (Bigstring.sub_shared ~len:(Iobuf.length iobuf) bigstring))
    | `Ok src ->
      if Iobuf.length iobuf < String.length src
      then return (Or_error.error_string "too much data received")
      else begin
        Iobuf.Fill.string iobuf src;
        load_loop ()
      end
  in
  load_loop () 

let check_status resp =
  match Response.status resp with
  | `OK | `Partial_content -> Ok ()
  | other -> Error (Error.create "HTTP response" other Code.sexp_of_status_code)

let check_length ~range bigstring =
  return (
    let actual_len = Bigstring.length bigstring in
    match range with
    | `exactly_pos_len (_, expect_len) ->
      if actual_len = expect_len
      then Ok ()
      else Or_error.errorf "response too short: %i < %i" actual_len expect_len
    | `all_with_max_len len ->
      assert (actual_len <= len);
      Ok ()
  )

let get uri ~interrupt ~range =
  let teardown = Ivar.create () in
  let (headers, max_len) =
    match range with
    | `exactly_pos_len (pos, len) ->
      let range_header = sprintf "bytes=%i-%i" pos (pos + len - 1) in
      (Header.init_with "range" range_header, len)
    | `all_with_max_len len -> (Header.init (), len)
  in
  let res =
    Monitor.try_with_join_or_error (fun () ->
      Client.get uri ~interrupt:(Ivar.read teardown) ~headers
      >>= fun (resp, body) ->
      let body = Body.to_pipe body in
      upon (Ivar.read teardown) (fun () -> Pipe.close_read body);
      return (check_status resp)
      >>=? fun () ->
      drain_pipe_to_bigstring ~max_len body
      >>=? fun bs ->
      check_length ~range bs
      >>=? fun () ->
      return (Ok bs)
    )
  in
  Deferred.any 
    [ res
    ; (interrupt >>| fun () -> Or_error.error_string "interrupt")
    ]
  >>= fun res ->
  Ivar.fill teardown ();
  return res

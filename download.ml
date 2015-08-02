open Core.Std
open Async.Std
open Cohttp
open Cohttp_async

(* I'd rather use Iobufs everywhere, but other bits need to take bigstrings.
 * The iobuf here is just a temporary one looking at the same backing bigstring
 * for convenience. *)
let drain_pipe_to_bigstring ~progress ~max_len pipe =
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
      then return (Error (Error.of_string "too much data received"))
      else begin
        Iobuf.Fill.string iobuf src;
        progress (Iobuf.capacity iobuf - Iobuf.length iobuf);
        load_loop ()
      end
  in
  load_loop () 

let check_status (resp, body) =
  match Response.status resp with
  | `OK | `Partial_content -> Ok body
  | other -> Error (Error.create "HTTP response" other Code.sexp_of_status_code)

let check_length ~range bigstring =
  return (
    let actual_len = Bigstring.length bigstring in
    match range with
    | `exactly_pos_len (_, expect_len) ->
      if actual_len = expect_len
      then Ok bigstring
      else Error (Error.of_string (sprintf "response too short: %i < %i" actual_len expect_len))
    | `all_with_max_len len ->
      assert (actual_len <= len);
      Ok bigstring
  )

let get uri ~timeout ~progress ~range =
  let (headers, max_len) =
    match range with
    | `exactly_pos_len (pos, len) ->
      let range_header = sprintf "bytes=%i-%i" pos (pos + len - 1) in
      (Header.init_with "range" range_header, len)
    | `all_with_max_len len -> (Header.init (), len)
  in
  let interrupt_ivar = Ivar.create () in
  let interrupt = Ivar.read interrupt_ivar in
  let res =
    Monitor.try_with_join_or_error (fun () ->
      Client.get uri ~interrupt ~headers
      >>| check_status
      >>|? Body.to_pipe
      >>=? fun pipe ->
      upon interrupt (fun () -> Pipe.close_read pipe);
      drain_pipe_to_bigstring ~progress ~max_len pipe
      >>=? check_length ~range
    )
  in
  let res =
    choose
      [ choice res Fn.id
      ; choice (Clock.after timeout) (fun () -> Error (Error.of_string "timeout"))
      ]
  in
  upon res (fun _ -> Ivar.fill_if_empty interrupt_ivar ());
  res
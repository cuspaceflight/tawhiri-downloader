open Core
open Async

(* Cohttp blows---in particular,
   doesn't have good timeout/interrupt support for requests.

   httpaf can't do SSL.

   OCurl's bindings to curl-multi look great, except integrating them with async is
   _really_ tricky. This is what we have for now... *)
type range =
  [ `exactly_pos_len of int * int
  | `all_with_max_len of int
  ]
[@@deriving sexp]

(* Curl.Multi will sometimes (the resolver?) close fds _before_ calling the socket
   function with POLL_REMOVE, which means it will already be gone from the epoll and
   removal will fail. I could not find a single ocaml epoll library that can tolerate
   this, and it's 11pm, so you have the following:

   Other ideas: dup the fds and let async take ownership of the dupp'd fd; rewrite in
   python; magic a good OCaml HTTP library out of somewhere. *)
module Epoll : sig
  type in_and_or_out =
    | In
    | Out
    | Inout

  type ctl_action =
    | Add of in_and_or_out
    | Del
    | Mod of in_and_or_out

  type t

  val create : unit -> t
  val ctl : t -> Core.Unix.File_descr.t -> ctl_action -> unit
  val epoll_fd : t -> Core.Unix.File_descr.t

  type epoll_wait_result =
    { fd : Core.Unix.File_descr.t
    ; in_ : bool
    ; out : bool
    ; hup : bool
    }

  val epoll_wait : t -> timeout:int -> epoll_wait_result option
end = struct
  module Raw = struct
    open Ctypes
    open Foreign

    let epoll_cloexec = 1 lsl 19
    let epoll_create1 = foreign "epoll_create" (int @-> returning int)
    let epoll_ctl_add = 1
    let epoll_ctl_del = 2
    let epoll_ctl_mod = 3
    let epollin = 1
    let epollhup = 16
    let epollout = 4

    type epoll_event

    let epoll_event : epoll_event structure typ = structure "epoll_event"
    let epoll_event_events = field epoll_event "events" uint32_t
    let epoll_event_data = field epoll_event "data" uint64_t
    let () = seal epoll_event

    let epoll_ctl =
      let raw =
        foreign
          ~check_errno:true
          "epoll_ctl"
          (int @-> int @-> int @-> ptr epoll_event @-> returning int)
      in
      fun t ~op ~fd ~events ~data ->
        let event = make epoll_event in
        setf event epoll_event_events (Unsigned.UInt32.of_int events);
        setf event epoll_event_data (Unsigned.UInt64.of_int data);
        raw t op fd (addr event)
    ;;

    let epoll_wait =
      let raw =
        foreign
          ~check_errno:true
          "epoll_wait"
          (int @-> ptr epoll_event @-> int @-> int @-> returning int)
      in
      fun t ~timeout ->
        let event = make epoll_event in
        (* XXX: We _should_ be making an array of events and passing that in.
           However, ctypes computes the length of [epoll_event] as 16, when it's actually
           12. I don't know if I'm doing something wrong or not. But for this app it just
           doesn't matter. This is so dumb. *)
        match raw t (addr event) 1 timeout with
        | 0 -> None
        | 1 ->
          let events = getf event epoll_event_events |> Unsigned.UInt32.to_int in
          let data = getf event epoll_event_data |> Unsigned.UInt64.to_int in
          Some (events, data)
        | other -> failwithf "epoll_wait with max=1 returned %i" other ()
    ;;
  end

  type in_and_or_out =
    | In
    | Out
    | Inout
  [@@deriving sexp_of]

  type ctl_action =
    | Add of in_and_or_out
    | Del
    | Mod of in_and_or_out
  [@@deriving sexp_of]

  type t = int

  let create () = Raw.epoll_create1 Raw.epoll_cloexec
  let epoll_fd t = Core.Unix.File_descr.of_int t

  let ctl t fd change =
    let op =
      match change with
      | Add _ -> Raw.epoll_ctl_add
      | Del -> Raw.epoll_ctl_del
      | Mod _ -> Raw.epoll_ctl_mod
    in
    let events =
      match change with
      | Add In | Mod In -> Raw.epollin
      | Add Out | Mod Out -> Raw.epollout
      | Add Inout | Mod Inout -> Raw.epollin lor Raw.epollout
      | Del -> 0
    in
    match
      Raw.epoll_ctl
        t
        ~op
        ~fd:(Core.Unix.File_descr.to_int fd)
        ~events
        ~data:(Core.Unix.File_descr.to_int fd)
    with
    | exception Unix.Unix_error (code, fn_name, str) ->
      let extra_info = sprintf !"%{sexp:ctl_action} %{Core.Unix.File_descr}" change fd in
      let str = 
        match str with
        | "" -> extra_info
        | str -> sprintf "%s %s" str extra_info
      in
      raise (Unix.Unix_error (code, fn_name, str))
    | 0 -> ()
    | _ -> failwith "epoll_ctl unexpected return value"
  ;;

  type epoll_wait_result =
    { fd : Core.Unix.File_descr.t
    ; in_ : bool
    ; out : bool
    ; hup : bool
    }

  let epoll_wait t ~timeout =
    match Raw.epoll_wait t ~timeout with
    | None -> None
    | Some (events, data) ->
      let fd = Core.Unix.File_descr.of_int data in
      Some
        { fd
        ; in_ = events land Raw.epollin <> 0
        ; out = events land Raw.epollout <> 0
        ; hup = events land Raw.epollhup <> 0
        }
  ;;
end

module Async_multi_integration = struct
  let () = Curl.global_init CURLINIT_GLOBALALL
  let curl_multi = Curl.Multi.create ()
  let result_ivars = String.Table.create ()

  let rec after_curl_actions () =
    match Curl.Multi.remove_finished curl_multi with
    | None -> ()
    | Some (curl_easy, curl_code) ->
      let result_ivar =
        Hashtbl.find_and_remove result_ivars (Curl.get_private curl_easy)
        |> Option.value_exn ~here:[%here]
      in
      Ivar.fill result_ivar curl_code;
      after_curl_actions ()
  ;;

  let setup_epoll_integration () =
    let epoll = Epoll.create () in
    let add_or_mod_epoll fd flags =
      (* I used to have a version of this where we'd track whether or not a fd was in the
         epoll, and appropriately invoke [Add] or [Mod] in order to effect the right
         change. And still we'd get ENOENT. I think curl was closing the file descriptors
         without telling us, but I'm not sure. In any case, I don't care.

         If we mess this up, most likely we'll just fail to track the fds for a specific
         request and time out, and then retry it. Worst case, we'll infinite loop repeatedly
         epolling the same fd, waking up, doing no work, and going back to sleep, and waking
         up immediately again, until it times out. *)
      try Epoll.ctl epoll fd (Mod flags) with
      | Unix.Unix_error (ENOENT, _, _) -> Epoll.ctl epoll fd (Add flags)
    in
    let socket_function_exn fd (poll : Curl.Multi.poll) =
      match poll with
      | POLL_NONE | POLL_REMOVE ->
        (* curl sometimes (the resolver?) calls this _after_ closing the fd. *)
        (try Epoll.ctl epoll fd Del with
        | Unix.Unix_error (EBADF, _, _) -> ())
      | POLL_IN -> add_or_mod_epoll fd In
      | POLL_OUT -> add_or_mod_epoll fd Out
      | POLL_INOUT -> add_or_mod_epoll fd Inout
    in
    Curl.Multi.set_socket_function curl_multi (fun fd poll ->
        (* ocurl throws away exns, so we have to send them up to the main monitor. *)
        match socket_function_exn fd poll with
        | exception exn -> Monitor.send_exn Monitor.main exn
        | () -> ());
    let epoll_async_fd =
      (* it's a little known fact that you can put an epoll inside an epoll. The epoll fd
         is "ready to read" whenever it has events pending. *)
      Fd.create (Socket `Unconnected) (Epoll.epoll_fd epoll) (Info.of_string "curl-epoll")
    in
    let on_epoll_ready () =
      match Epoll.epoll_wait epoll ~timeout:0 with
      | None -> failwith "epoll is ready, but epoll_wait yielded nothing?"
      | Some { fd; in_; out; hup } ->
        let fd_status : Curl.Multi.fd_status =
          match in_ || hup, out with
          | false, false -> EV_AUTO
          | true, false -> EV_IN
          | false, true -> EV_OUT
          | true, true -> EV_INOUT
        in
        ignore (Curl.Multi.action curl_multi fd fd_status : int);
        after_curl_actions ()
    in
    don't_wait_for
      (let%bind reason = Fd.every_ready_to epoll_async_fd `Read on_epoll_ready () in
       Monitor.send_exn
         Monitor.main
         (Exn.create_s
            [%message
              "curl-epoll every_ready_to failed"
                (reason : [ `Bad_fd | `Closed | `Unsupported ])]);
       return ())
  ;;

  let setup_timer_integration () =
    let timer_event = ref None in
    let action_timeout () =
      Curl.Multi.action_timeout curl_multi;
      after_curl_actions ()
    in
    let timer_function_exn after_millis =
      (* there's a reentrancy bug in time_source where if you try to reschedule an event
         from within its fire function, it will just be dropped. Sigh.

         Avoid by explicitly aborting and recreating, rather than using
         [reschedule_after]. We should be able to use [reschedule_after] once it's fixed. *)
      (match !timer_event with
      | None -> ()
      | Some event ->
        ignore (Clock_ns.Event.abort event () : _ Clock_ns.Event.Abort_result.t));
      match after_millis with
      | -1 ->
        (* delete the timer *)
        ()
      | after_millis ->
        let after = Time_ns.Span.of_ms (float after_millis) in
        timer_event := Some (Clock_ns.Event.run_after after action_timeout ())
    in
    Curl.Multi.set_timer_function curl_multi (fun millis ->
        match timer_function_exn millis with
        | exception exn -> Monitor.send_exn Monitor.main exn
        | () -> ())
  ;;

  let initialise_once =
    Memo.unit (fun () ->
        setup_epoll_integration ();
        setup_timer_integration ())
  ;;

  let generate_curl_easy_id =
    let next = ref 0 in
    fun () ->
      let result = Int.to_string !next in
      incr next;
      result
  ;;

  let perform curl_easy =
    let id = generate_curl_easy_id () in
    Curl.set_private curl_easy id;
    let result_ivar = Ivar.create () in
    Hashtbl.add_exn result_ivars ~key:id ~data:result_ivar;
    Curl.Multi.add curl_multi curl_easy;
    Ivar.read result_ivar
  ;;

  let remove_idempotent curl_easy =
    (* idempotent: *)
    match Hashtbl.find_and_remove result_ivars (Curl.get_private curl_easy) with
    | None -> (* already removed by remove_finished *) ()
    | Some _ -> Curl.Multi.remove curl_multi curl_easy
  ;;
end

let get url ~interrupt ~range =
  let output_buffer =
    let (`exactly_pos_len (_, len) | `all_with_max_len len) = range in
    Iobuf.create ~len
  in
  Async_multi_integration.initialise_once ();
  let curl_easy = Curl.init () in
  Curl.set_url curl_easy url;
  (match range with
  | `all_with_max_len _ -> ()
  | `exactly_pos_len (pos, len) ->
    let range_header = sprintf "Range: bytes=%i-%i" pos (pos + len - 1) in
    Curl.set_httpheader curl_easy [ range_header ]);
  let response_too_long_ivar = Ivar.create () in
  Curl.set_writefunction curl_easy (fun s ->
      (match Iobuf.Fill.stringo output_buffer s with
      | exception _ -> Ivar.fill_if_empty response_too_long_ivar ()
      | () -> ());
      String.length s);
  let%bind result =
    choose
      [ choice (Async_multi_integration.perform curl_easy) (fun r -> `Complete r)
      ; choice (Ivar.read response_too_long_ivar) (fun () -> `Response_too_long)
      ; choice interrupt (fun () -> `Interrupt)
      ]
  in
  Async_multi_integration.remove_idempotent curl_easy;
  let classified_curl_result =
    match result with
    | `Complete CURLE_OK ->
      let code = Curl.get_responsecode curl_easy in
      (match code >= 200 && code < 300 with
      | true -> Ok ()
      | false -> error_s [%message "HTTP error code from server" ~url (code : int)])
    | `Complete error ->
      error_s [%message "curl error" ~url (range : range) ~error:(Curl.strerror error)]
    | `Response_too_long -> error_s [%message "response too long" ~url (range : range)]
    | `Interrupt -> Or_error.error_string "interrupted"
  in
  Curl.cleanup curl_easy;
  let curl_easy = `Cleaned_up in
  let `Cleaned_up = curl_easy in
  match classified_curl_result with
  | Error _ as error -> return error
  | Ok () ->
    Iobuf.flip_lo output_buffer;
    let length_is_ok =
      match range with
      | `all_with_max_len _ -> true
      | `exactly_pos_len (_, expected) -> Iobuf.length output_buffer = expected
    in
    (match length_is_ok with
    | true -> return (Ok (Iobuf.Expert.to_bigstring_shared output_buffer))
    | false ->
      return
        (error_s
           [%message
             "response too short"
               ~url
               (range : range)
               ~actual:(Iobuf.length output_buffer : int)]))
;;

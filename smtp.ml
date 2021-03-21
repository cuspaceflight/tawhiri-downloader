open Core
open Async

let rec read_response ?(accept_354 : unit option) pipe =
  match%bind Pipe.read pipe with
  | `Eof -> return (Or_error.error_string "EOF")
  | `Ok line ->
    if String.length line < 4
    then return (Or_error.errorf "Line too short: %s" line)
    else (
      match String.to_list (String.sub ~pos:0 ~len:4 line) with
      | [ _; _; _; '-' ] -> read_response ?accept_354 pipe
      | [ '2'; _; _; ' ' ] -> return (Ok ())
      | [ '3'; '5'; '4'; ' ' ] when Option.is_some accept_354 -> return (Ok ())
      | _ -> return (Or_error.error_string line))
;;

let read_response_exn ?accept_354 pipe =
  match%bind read_response ?accept_354 pipe with
  | Error error -> Error.raise error
  | Ok response -> return response
;;

let server = Socket.Address.Inet.create Unix.Inet_addr.localhost ~port:25

let writer_lines_crlf w =
  Pipe.create_writer (fun p ->
      Writer.transfer w p (fun s ->
          Writer.write w s;
          Writer.write w "\r\n"))
;;

let is_legal_line = function
  | "." -> Or_error.error_string "Can't have . on its own line"
  | s when String.length s > 800 -> Or_error.error_string "line too long"
  | s ->
    let rec loop s n =
      if n >= String.length s
      then Ok ()
      else (
        match String.get s n with
        | '\t' | ' ' .. '~' -> loop s (n + 1)
        | c -> Or_error.errorf "Illegal character %i" (Char.to_int c))
    in
    loop s 0
;;

let rec are_legal_lines = function
  | [] -> Ok ()
  | line :: lines ->
    (match is_legal_line line with
    | Error _ as error -> error
    | Ok () -> are_legal_lines lines)
;;

let default_helo = "tawhiri-downloader"
let default_from = "tawhiri-downloader@localhost"

let send_mail
    ?(helo = default_helo)
    ?(mail_from = default_from)
    ~rcpt_to
    ~subject
    ~data
    ()
  =
  let lines =
    sprintf "Subject: %s" subject
    ::
    sprintf "From: %s" mail_from
    ::
    sprintf "To: %s" (String.concat rcpt_to ~sep:", ")
    :: sprintf "Content-type: text/plain; charset=UTF-8" :: "" :: String.split_lines data
  in
  match are_legal_lines lines, rcpt_to with
  | (Error _ as err), _ -> return err
  | Ok (), [] -> return (Or_error.error_string "rcpt_to empty")
  | Ok (), (_ :: _ as rcpt_to) ->
    Monitor.try_with_or_error (fun () ->
        Tcp.with_connection
          (Tcp.Where_to_connect.of_inet_address server)
          (fun _ reader writer ->
            let reader = Reader.lines reader in
            let writer = writer_lines_crlf writer in
            let command ?accept_354 s =
              let%bind () = Pipe.write writer s in
              read_response_exn reader ?accept_354
            in
            let%bind () = read_response_exn reader in
            let%bind () = command ("HELO " ^ helo) in
            let%bind () = command ("MAIL FROM:" ^ mail_from) in
            let%bind () =
              Deferred.List.iter rcpt_to ~how:`Sequential ~f:(fun s ->
                  command ("RCPT TO:" ^ s))
            in
            let%bind () = command ~accept_354:() "DATA" in
            let%bind () =
              Deferred.List.iter lines ~how:`Sequential ~f:(Pipe.write writer)
            in
            let%bind () = command "." in
            let%bind () = command "QUIT" in
            Pipe.closed reader))
;;

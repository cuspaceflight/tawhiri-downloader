open Core.Std
open Async.Std
open Cohttp
open Cohttp_async
open Common

let test () =
  ignore Download.get_message

let cmd = 
  Command.async
    ~summary:"Test"
    Command.Spec.empty
    (fun () ->
      test () 
      >>= fun r ->
      begin
        match r with
        | Ok _ -> Log.Global.info "all ok";
        | Error e -> Error.raise e
      end;
      Log.Global.flushed ()
    )

let () = Command.run cmd

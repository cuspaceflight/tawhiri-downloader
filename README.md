As root

    apt install -y build-essential rsync git libpcre3-dev libncurses-dev pkg-config m4 unzip aspcud autoconf bubblewrap
    apt install -y libssl-dev libgmp-dev libffi-dev libeccodes-dev libcurl4-gnutls-dev
    sh <(curl -sL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)

As your user

    opam init -y
    eval $(opam env)
    opam install core async ctypes ctypes-foreign ocurl

I last built this on 2021-03-21, using debian 10 (buster), ocaml 4.12.0, core v0.14.1, async v0.14.0, ctypes 0.18.0 and ocurl 0.9.1.

Now build

    dune build --profile=release main.exe

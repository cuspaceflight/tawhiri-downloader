```
apt install -y build-essential rsync git libpcre3-dev libncurses-dev pkg-config m4 unzip aspcud autoconf bubblewrap
opam init --comp=4.07.0 -y
eval `opam env`

apt install -y libssl-dev libffi-dev libgrib-api-dev
# This patch on top of cohttp ensures allows using ~interrupt to abort an in-progress
# HTTP GET (the un-patched cohttp ~interrupt can only abort the 'connect' step)
opam pin cohttp.2.1.2 'https://github.com/danielrichman/ocaml-cohttp.git#v2.1.2-drichman'
opam pin cohttp-async.2.1.2 'https://github.com/danielrichman/ocaml-cohttp.git#v2.1.2-drichman'
opam install core async async_ssl cohttp-async ctypes ctypes-foreign
dune build --profile=release main.exe
```

```
apt install libssl-dev libffi-dev libgrib-api-dev 
opam pin cohttp-async 'https://github.com/danielrichman/ocaml-cohttp.git#v1.2.0-drichman'
opam install core async async_ssl cohttp-async ctypes ctypes-foreign
./build.sh
```

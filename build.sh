# apt-get install libgrib-api-dev libffi-dev
# opam pin cohttp https://github.com/danielrichman/ocaml-cohttp#v0.20.2-hotpatch
# opam install core async cohttp ctypes ctypes-foreign 

corebuild \
    -pkg ctypes.foreign \
    -pkg async \
    -pkg cohttp.async \
    -lflags -cclib,-lgrib_api \
    main.native 

#!/bin/bash
corebuild \
    -pkg ctypes.foreign \
    -pkg async \
    -pkg cohttp.async \
    -lflags -cclib,-lgrib_api \
    main.native 

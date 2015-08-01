corebuild \
    -pkg ctypes.foreign \
    -pkg async \
    -pkg cohttp.async \
    -pkg custom_printf.syntax \
    -pkg pa_pipebang \
    -lflags -cclib,-lgrib_api \
    test.native 

# Preparing web100 source

Install go-bindata:

    go get -u github.com/jteeuwen/go-bindata/...

Convert tcp-kis.txt into embedded go file:

    go-bindata -prefix embed/ -pkg web100 -o tcpkis.go embed

# TODOs

 * Upstream the web100.c changes to support 64-bit environments.
 * Upstream the tcp-kis.txt changes to eliminate conflicts for StartTimeSec and
   StartTimeUsec (and other inconsistencies across published versions).
 * Optimize web100.go implementation, e.g. to read snaplog from byte buffer
   instead of from file.

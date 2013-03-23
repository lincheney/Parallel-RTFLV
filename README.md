Parallel-RTFLV
==============
v1.3.2

Download segments of a real-time FLV stream (via HTTP) in parallel.

Catering specifically for http://sbsauvod-f.akamaihd.net/.

example.py contains an example command line program with usage:

    python example.py url outfile parts [--debug | --no-resume | --lock]

e.g. python example.py http://sbsauvod-f.akamaihd.net/... video.flv 5

Windows 32-bit binary for v1.3.2 is at https://github.com/lincheney/Parallel-RTFLV/raw/gh-pages/RTFLV.zip
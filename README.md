Parallel-RTFLV
==============

Download segments of a real-time FLV stream (via HTTP) in parallel.

Catering specifically for http://sbsauvod-f.akamaihd.net/.

example.py contains an example command line program with usage:

    python example.py url outfile parts [--debug]

e.g. python example.py http://sbsauvod-f.akamaihd.net/... video.flv 5

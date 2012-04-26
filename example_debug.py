#
#       example_debug.py
#       
#       Example command line program making use of
#       Parallel_RTFLV (with debug on)
#       
#       Usage: python example.py url outfile parts
#       
#       url:            url of FLV stream - where seeking is done
#                       by appending &seek=123
#       outfile:        filename to save to
#       parts:          number of parts to split up downloading
#
#       No stats are printed - debugging messages only.
#

import sys
from threading import Thread
from Parallel_RTFLV import save_stream

if (len (sys.argv) < 4):
    print "Usage: python {} url outfile parts".format (sys.argv[0])
    sys.exit (0)

url, outfile, parts = sys.argv[1 : 4]
parts = int (parts)
# function to make url
url_fn = lambda time: url + "&seek=" + str (time)

save_stream (url_fn, outfile, parts, debug = True)

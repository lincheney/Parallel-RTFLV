#
#       example.py
#       
#       Example command line program making use of
#       Parallel_RTFLV
#       
#       Usage: python example.py url outfile duration parts
#       
#       url:            url of FLV stream - where seeking is done
#                       by appending &seek=123
#       outfile:        filename to save to
#       duration:       duration of video to download (or estimate;
#                       if estimate, overestimate is better)
#       parts:          number of parts to split up downloading
#
#       Simple statistics are printed out in a 'tabular' format
#       If any one part fails, everything stops
#

import sys
from Parallel_RTFLV import save_stream

if (len (sys.argv) < 3):
    print "Usage: python {} url outfile duration parts".format (sys.argv[0])
    sys.exit (0)

url, outfile, duration, parts = sys.argv[1 : 5]
duration = int (duration)
parts = int (parts)

url_fn = lambda time: url + "&seek=" + str (time)

# header for stats
for i in range (parts):
    print "Part {:2}  ".format (i),
print

# running stats for each part
progress = ["{:<9}".format (0)] * parts

for message in save_stream (url_fn, outfile, duration, parts):
    part = message["part"]
    if ("status" in message):
        status = message["status"]
        progress[part] = "{:<9}".format (status)
        
        if (status == "Failed"):
            print "\nDownload of part {} failed. Aborting...".format (part)

    if ("timestamp" in message):
        part_progress = message["timestamp"] * parts / duration / 10.0
        progress[part] = "{:<9}".format (part_progress)
    
    # print out the stats
    print "\r" + " ".join (progress),
    sys.stdout.flush ()

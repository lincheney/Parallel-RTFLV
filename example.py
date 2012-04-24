#
#       example.py
#       
#       Example command line program making use of
#       Parallel_RTFLV
#       
#       Usage: python example.py url outfile parts
#       
#       url:            url of FLV stream - where seeking is done
#                       by appending &seek=123
#       outfile:        filename to save to
#       parts:          number of parts to split up downloading
#
#       Simple statistics are printed out in a 'tabular' format
#       If any one part fails, everything stops
#

import sys
from threading import Thread
import Queue
from Parallel_RTFLV import save_stream

if (len (sys.argv) < 2):
    print "Usage: python {} url outfile parts".format (sys.argv[0])
    sys.exit (0)

url, outfile, parts = sys.argv[1 : 4]
parts = int (parts)
# function to make url
url_fn = lambda time: url + "&seek=" + str (time)

# thread the function
mainqueue = Queue.Queue ()
thread = Thread (target = save_stream,
                 args = (url_fn, outfile, parts, mainqueue) )
thread.daemon = True
thread.start ()

# expecting -1 for failure, otherwise dict with filesize, duration
message = mainqueue.get ()
if (message == -1):
    print "Download failed"
    sys.exit (1)
print message

# running stats for each part
progress = ["{:<9}".format (0)] * parts
# header for stats
for i in range (parts):
    print "Part {:2}  ".format (i),
print

while (True):
    message = mainqueue.get ()
    part = message["part"]
    if ("status" in message):
        status = message["status"]
        
        if (status == -1):
            print "\nDownload of part {} failed. Aborting...".format (part)
            progress[part] = "{:<9}".format ("Failed")
            break
        elif (status == 1):
            progress[part] = "{:<9}".format ("Done")
        elif (status == 2):
            # all parts finished; waiting to join files
            progress[part] = "{:<9}".format ("Done")
            print "\r" + " ".join (progress)
            print "Joining files..."
            break

    if ("progress" in message):
        progress[part] = "{:<9.2f}".format (message["progress"] * 100)
    
    # print out the stats
    print "\r" + " ".join (progress),
    sys.stdout.flush ()

# wait for thread to finish
thread.join ()

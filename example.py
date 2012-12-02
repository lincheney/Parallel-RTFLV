#
#       example.py
#       
#       Example command line program making use of
#       Parallel_RTFLV
#       
#       Usage: python example.py url outfile parts [--debug]
#       
#       url:            url of FLV stream - where seeking is done
#                       by appending &seek=123
#       outfile:        filename to save to
#       parts:          number of parts to split up downloading
#       debug:          if the option is included, only debug messages
#                       will be printed
#
#       If any one part fails, everything stops
#

import sys
from Parallel_RTFLV import MultiPart_Downloader

if (len (sys.argv) < 4):
    print "Usage: python {} url outfile parts [--debug]".format (sys.argv[0])
    sys.exit (0)

url, outfile, parts = sys.argv[1 : 4]
debug = (len (sys.argv) >= 5 and sys.argv[4] == "--debug")
parts = int (parts)
# function to make url
url_fn = lambda time: url + "&seek=" + str (time)

# running stats for each part
stat_strs = [""] * parts
stat_printed = False

#
#       set_stats:
#       @part:          part
#       @stat:          status/progress of @part
#       
#       Sets progress/stats for @part in stat_strs
#
def set_stats (part, stat):
    stat_strs[part] = "P{:<2}:{:<6}".format (part, stat)

# set initial progress to 0 for all parts
for part in range (parts):
    set_stats (part, 0)

def check_stat_printed ():
    global stat_printed
    if (stat_printed):
        print
    stat_printed = False

#
#       print_stats:
#       
#       Prints progress/stats for each part
#
def print_stats ():
    global stat_printed
    # print out the stats
    print "\r" + " ".join (stat_strs),
    sys.stdout.flush ()
    stat_printed = True

# various signal handlers
def got_filesize (filesize):
    check_stat_printed ()
    print "Filesize:", filesize

def got_duration (duration):
    check_stat_printed ()
    print "Duration:", duration

def part_finished (part):
    set_stats (part, "Done")
    print_stats ()

def part_failed (part):
    set_stats (part, "Failed")
    print_stats ()

def print_progress (progress, part):
    set_stats (part, "{:.2f}".format (progress * 100) )
    print_stats ()

def status_changed (status):
    check_stat_printed ()    
    if (status == -1):
        print "Downloading failed. Aborting"
    elif (status == 1):
        print "Downloading finished"
    elif (status == 2):
        print "Joining files..."
    elif (status == 3):
        print "Joining finished"

def got_debug_message (message, part):
    check_stat_printed ()
    if (part == None):
        sys.stderr.write (message + "\n")
    else:
        sys.stderr.write ("Part {}: {}\n".format (part, message) )

# make a downloader and connect to all signals
downloader = MultiPart_Downloader ()
if (debug):
    downloader.connect ("debug", got_debug_message)
else:
    downloader.connect ("got-duration", got_duration)
    downloader.connect ("got-filesize", got_filesize)
    downloader.connect ("part-finished", part_finished)
    downloader.connect ("part-failed", part_failed)
    downloader.connect ("status-changed", status_changed)

downloader.connect ("progress", print_progress)

# download the video
downloader.save_stream (url_fn, outfile, parts)

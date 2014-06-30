#
#       example.py
#       
#       Example command line program making use of
#       Parallel_RTFLV
#       
#       Usage: python example.py url outfile parts [--debug | --no-resume | --lock]
#       
#       url:            url of FLV stream - where seeking is done
#                       by appending &seek=123
#       outfile:        filename to save to
#       parts:          number of parts to split up downloading
#       debug:          debug messages will be printed
#       no-resume:      do not attempt to resume
#       lock:           make exclusive lock to outfile
#
#       If any one part fails, everything stops
#

import sys
from Parallel_RTFLV import MultiPart_Downloader

if len(sys.argv) < 4:
    print "Usage: python {} url outfile parts [--debug | --no-resume | --lock]".format(sys.argv[0])
    sys.exit(0)

url, outfile, parts = sys.argv[1:4]
parts = int(parts)

debug = ("--debug" in sys.argv[4:])
no_resume = ("--no-resume" in sys.argv[4:])
lock = ("--lock" in sys.argv[4:])

# function to make url
def url_fn(time):
    return "{}&seek={}".format(url, time)

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
def set_stats(part, stat):
    stat_strs[part] = "{0:<6}".format(stat)

# set initial progress to 0 for all parts
for part in range(parts):
    set_stats(part, 0)

def print_non_stat(*output):
    global stat_printed
    if stat_printed:
        print
        for i in output:
            print i,
    stat_printed = False

#
#       print_stats:
#       
#       Prints progress/stats for each part
#
def print_stats():
    global stat_printed
    # print out the stats
    if not stat_printed:
        print " ".join("P{0:<5}".format(i) for i in range(parts) )
    print "\r" + " ".join(stat_strs),
    sys.stdout.flush()
    stat_printed = True

# various signal handlers
def got_filesize(filesize):
    print_non_stat("Filesize:", filesize)

def got_duration(duration):
    print_non_stat("Duration:", duration)

def part_finished(part):
    set_stats(part, "Done")
    print_stats()

def part_failed(part):
    set_stats(part, "Failed")
    print_stats()

def print_progress(progress, part):
    set_stats(part, "{:.2f}".format(progress * 100) )
    print_stats()

def status_changed(status):
    check_stat_printed()
    if status == -1:
        print "Downloading failed. Aborting"
    elif status == 1:
        print "Downloading finished"
    elif status == 2:
        print "Joining files..."
    elif status == 3:
        print "Joining finished"

def got_debug_message(message, part):
    print_non_stat()
    if part == None:
        sys.stderr.write(message + "\n")
    else:
        sys.stderr.write("Part {}: {}\n".format(part, message) )

# make a downloader and connect to all signals
downloader = MultiPart_Downloader()
if debug:
    downloader.connect("debug", got_debug_message)
else:
    downloader.connect("got-duration", got_duration)
    downloader.connect("got-filesize", got_filesize)
    downloader.connect("part-finished", part_finished)
    downloader.connect("part-failed", part_failed)

downloader.connect("progress", print_progress)
downloader.connect("info", got_debug_message)

# download the video
print "Saving {}\nto {}".format(url, outfile)
downloader.save_stream(url_fn, outfile, parts, no_resume = no_resume, lock = lock)

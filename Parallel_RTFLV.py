#
#       Parallel_RTFLV:
#       
#       API to download different segments of a real-time FLV
#       stream in parallel (if this is possible) and
#       finally join them.
#       
#       In this way, a 1-hour video that would normally stream
#       for 1-hour can be downloaded in three 20min downloads
#       that occur in parallel.
#

import os
import shutil
import urllib2
import struct
import Queue
from threading import Thread, Lock

print_lock = Lock ()

def print_obj (obj):
    print_lock.acquire ()
    print obj
    print_lock.release ()

#
#       read_header:
#       @stream:        file-like object
#       
#       Returns:        9-byte header + 4-byte tag size (0)
#                       read from @stream
#
def read_header (stream):
    # header, tag size
    return stream.read (9 + 4)

#
#       read_tags:
#       @stream:        file-like object
#
#       Reads FLV-tags from @stream
#
#       Yields:         (@data, @_type, @timestamp, @body)
#
#                       @data: byte-string of the whole tag
#                       @_type: first byte of the tag
#                       @timestamp: timestamp of tag
#                       @body:  body of tag - the actual
#                               audio/video/metadata packet (byte-string)
#
def read_tags (stream):
    while (True):
        # type, size, timestamp, streamid
        data = stream.read (1 + 3 + 3 + 4)
        if (len (data) != 11):
            break
        
        _type = data[0]
        size = struct.unpack ("!I", "\x00" + data[1 : 4])[0]
        timestamp = struct.unpack ("!I", "\x00" + data[4 : 7])[0]
        streamid = struct.unpack ("!I", data[7 : 11])[0]
        
        body = stream.read (size)
        fullsize = stream.read (4)
        if (len (body) != size or len (fullsize) != 4):
            break
        data += body + fullsize
        fullsize = struct.unpack ("!I", fullsize)[0]
        
        yield (data, _type, timestamp, body)

#
#       set_tag_timestamp:
#       @data:          byte-string of FLV tag
#       @timestamp:     new timestamp to set on @data
#
#       Returns:        @data, but with @timestamp as the timestamp
#
def set_tag_timestamp (data, timestamp):
    return data[0 : 4] + struct.pack ("!I", timestamp)[1 : ] + data[7 : ]

#
#       get_metadata_number:
#       @metadata:      metadata block (body of FLV tag)
#       @key:           key in metadata
#
#       Returns:        the 64-bit float associated with @key
#                       found in @metadata
#                       if not found, returns None
#
def get_metadata_number (metadata, key):
    length_str = struct.pack ("!I", len (key) )[2 : ]
    index = metadata.find (length_str + key)
    if (index == -1):
        return None
    index += len (key) + 3
    return struct.unpack ("!d", metadata[index : index + 8])[0]

#
#       save_stream_part:
#       @url:           URL to open FLV stream
#       @filename:      filename to save to
#       @part:          which number part this is
#       @inqueue:       #Queue to receive input from
#       @outqueue:      #Queue to send output to
#       @debug:         Print debug messages
#
#       Downloads a part of an FLV stream given in @url to
#       @filename
#       
#       The function will return if it receives -1 on @inqueue.
#       The function will report its status and 0<=progress<=1
#       via @outqueue with a dict.
#       status=1 if download finished
#       status=-1 if download failed
#
def save_stream_part (url, filename, part, inqueue, outqueue, debug):
    if (debug):
        print_obj ("Part {}: Opening {}".format (part, url) )
    try:
        stream = urllib2.urlopen (url)
    except IOError as e:
        print_obj ("Part {0}: Failed to open {1}\n"
                   "Part {0}: {2}\n"
                   "Part {0}: Aborting".format (part, url, e) )
        outqueue.put ({"part" : part, "status" : -1})
        return
    # non-flv: fail
    if (stream.info ().gettype () != "video/x-flv"):
        stream.close ()
        if (debug):
            print_obj ("Part {}: {} is {} not FLV".format (
                part, url, stream.info ().gettype () ) )
        outqueue.put ({"part" : part, "status" : -1})
        return
    
    outfile = open (filename, "w")
    if (debug):
        print_obj ("Part {}: Created {}".format (part, filename) )
    # read the one header ...
    header = read_header (stream)
    if (len (header) != 13):
        if (debug):
            print_obj ("Part {}: FLV header (+ tag size) "
                       "is NOT 13 bytes. Aborting".format (part) )
        outqueue.put ({"part" : part, "status" : -1})
        return
    if (debug):
        print_obj ("Part {}: Read FLV header".format (part) )
    if (part == 0):
        outfile.write (header)
        if (debug):
            print_obj ("Part {}: Wrote FLV header".format (part) )
    
    # timestamps start at 0 no matter what so
    # need to offset timestamps
    offset = 0
    # end_time specifies the absolute end of this segment
    end_time = float ("inf")
    # duration specifies the relative end of this segment
    # whenever end_time, offset change, duration must be re-calculated
    duration = (end_time - offset) * 1000
    
    # ... and read all the tags
    for data, _type, timestamp, body in read_tags (stream):
        do_write = True
        if (_type == "\x12"):
            do_write = False
            # only bother with the initial sets of metadata
            if (timestamp == 0):
                if (part == 0):
                    # for part 0, grab filesize, duration
                    full_duration = get_metadata_number (body, "duration")
                    filesize = get_metadata_number (body, "filesize")
                    
                    if (full_duration != None and filesize != None):
                        if (debug):
                            print_obj ("Part {}: Found duration ({}), "
                                    "filesize ({})".format (
                                    part, full_duration, filesize ) )
                        outqueue.put ({"full-duration" : full_duration,
                                       "filesize" : filesize})
                
                # the timeBase in a metadata tag indicates the ACTUAL
                # starting time of this segment - use this as offset
                new_offset = get_metadata_number (body, "timeBase")
                if (new_offset != None):
                    offset = new_offset
                    duration = (end_time - offset) * 1000
                    if (debug):
                        print_obj ("Part {}: Found timeBase ({}) "
                                   "- new duration ({})".format (
                                   part, offset, duration) )
                    outqueue.put ({"part" : part, "offset" : offset})
            elif (debug):
                print_obj ("Part {}: Ignoring new metadata at {}".format (
                           part, timestamp) )
        elif (_type == "\x08"):
            if ((ord (body[0]) >> 4) == 10):
                # aac sequence header - only write if first part
                do_write = (body[1] != "\x00")
        elif (_type == "\x09"):
            if ((ord (body[0]) & 0xf) == 7):
                # avc sequence header - only write if first part
                do_write = (body[1] != "\x00")
        
        # write to file if first part or not metadata/sequence header
        if (do_write or part == 0):
            data = set_tag_timestamp (data, timestamp + offset * 1000)
            outfile.write (data)
        
        # reached @duration ?
        if (timestamp >= duration):
            if (debug):
                print_obj ("Part {}: Finished at {}".format (part, duration) )
            break
        
        # report progress for audio, video tags
        if (_type == "\x08" or _type == "\x09"):
            if (duration == float ("inf") ):
                progress = 0
            else:
                progress = timestamp / duration
            outqueue.put ({"part" : part, "progress" : progress})
        
        try:
            message = inqueue.get_nowait ()
            if (message == -1):
                # we've been told to stop, so fail
                if (debug):
                    print_obj ("Part {}: Aborting".format (part) )
                outfile.close ()
                stream.close ()
                outqueue.put ({"part" : part, "status" : -1})
                return
            # otherwise, message is a new end_time
            end_time = message
            duration = (end_time - offset) * 1000
            if (debug):
                print_obj ("Part {}: Got new end_time ({}) "
                           "- new duration ({})".format (
                           part, end_time, duration) )
        except Queue.Empty:
            pass
    # finished reading all that's needed - success!
    outfile.close ()
    stream.close ()
    if (debug):
        print_obj ("Part {}: Done".format (part) )
    outqueue.put ({"part" : part, "status" : 1})

#
#       save_stream:
#       @url_fn:        function that returns a URL for a given seek-time
#       @filename:      filename to save FLV to
#       @parts:         number of parts in which to download FLV
#       @mainqueue:     queue to put messages on
#       @duration:      total duration of FLV to download
#       @debug:         Print debug messages
#
#       Downloads the FLV stream from @url_fn in several parts and save to
#       @filename.
#       Specify @duration if not downloading full video.
#       
#       The function can optionally report progress/status to @mainqueue
#       (in which case it should be threaded). First message is -1 on
#       on failure, dict with keys "duration", "filesize" on success.
#       All other tags are same as for save_stream_part(), but
#       status=2 if ALL parts are finished
#       
#       The function will abort if any one part fails.
#       
#       If the download was successful, the partial files are joined
#       into @filename (and then deleted).
#
def save_stream (url_fn, filename, parts = 3,
                 mainqueue = None, duration = float ("inf"), debug = False):
    # list of [thread, queue, finished?]
    threads = list ()
    inqueue = Queue.Queue ()
    
    # start part 0 first to get filesize, duration
    outqueue = Queue.Queue ()
    # url for this part
    part_url = url_fn (0)
    thread = Thread (target = save_stream_part,
                     args = (url_fn (0), filename, 0,
                             outqueue, inqueue, debug) )
    threads.append ([thread, outqueue, False])
    thread.daemon = True
    thread.start ()
    if (debug):
        print_obj ("Part 0 started")
    
    # expecting first message with filesize, duration; error otherwise
    message = inqueue.get ()
    if ("filesize" not in message or "full-duration" not in message):
        if (debug):
            print_obj ("Found no filesize, duration metadata. Aborting")
        if (mainqueue != None):
            mainqueue.put (-1)
        return
    duration = min (message["full-duration"], duration)
    if (mainqueue != None):
        mainqueue.put ({"duration" : duration,
                        "filesize" : message["filesize"]})
    
    if (debug):
        print_obj ("Starting all other parts")
    # now that we have duration, we can start all other parts
    part_duration = duration / parts
    for i in range (1, parts):
        outqueue = Queue.Queue ()
        # url for this part
        part_url = url_fn (i * part_duration)
        part_filename = filename + ".part" + str (i)
        thread = Thread (target = save_stream_part,
                         args = (part_url, part_filename, i,
                                 outqueue, inqueue, debug) )
        threads.append ([thread, outqueue, False])
        thread.daemon = True
        thread.start ()
        if (debug):
            print_obj ("Part {} started".format (i) )
    # add 0.5 just in case
    threads[-1][1].put (duration + 0.5)
    
    while (True):
        message = inqueue.get ()
        part = message["part"]
        
        if ("status" in message):
            status = message["status"]
            # this part failed, so abort all
            if (status == -1):
                if (debug):
                    print_obj ("Part {} failed. Aborting".format (part) )
                for i in threads:
                    i[1].put (-1)
                if (mainqueue != None):
                    mainqueue.put (message)
                return
            
            # this part is done, check if all others are done too
            if (status == 1):
                threads[part][2] = True
                if (all (x[2] for x in threads) ):
                    message["status"] = 2
                    if (debug):
                        print_obj ("All parts done")
                    if (mainqueue != None):
                        mainqueue.put (message)
                    break
                if (debug):
                    print_obj ("Part {} done".format(part) )
        
        if ("offset" in message):
            # got an offset for part: this is end_time for part-1
            if (part > 0):
                threads[part - 1][1].put (message["offset"])
        if (mainqueue != None):
            mainqueue.put (message)
    
    if (debug):
        print_obj ("Starting to join files")
    # join all files and delete partials
    # first part is contained in @filename
    # others in @filename.partX
    ofile = open (filename, "a")
    for i in range (1, parts):
        part_filename = filename + ".part" + str (i)
        
        partfile = open (part_filename, "r")
        shutil.copyfileobj (partfile, ofile)
        partfile.close ()
        if (debug):
            print_obj ("Appended part {} : {}".format (i, part_filename) )
        
        os.remove (part_filename)
        if (debug):
            print_obj ("Deleted part {}  : {}".format (i, part_filename) )
    ofile.close ()
    if (debug):
        print_obj ("Joining done")

#
#       Parallel_RTFLV:
#       
#       API to download different segments of a real-time FLV
#       stream in parallel (if this is possible) and
#       finally join them (not yet implemented).
#       
#       In this way, a 1-hour video that would normally stream
#       for 1-hour can be downloaded in three 20min downloads
#       that occur in parallel.
#

import urllib2
import struct
import Queue
from threading import Thread

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
#       save_stream_part:
#       @url:           URL to open FLV stream
#       @filename:      base filename - .part6 etc. is appended to this
#       @duration:      duration to stream (sec)
#       @part:          which number part this is
#       @inqueue:       #Queue to receive input from
#       @outqueue:      #Queue to send output to
#       @report_time:   interval to report stats to @outqueue (msec)
#
#       Downloads a part of an FLV stream given in @url to
#       filename.part(@part) (e.g. john.flv.part3) of the given @duration.
#       
#       The function will return if it receives -1 on @inqueue.
#       The function will report its status and 0<=progress<=1
#       via @outqueue with a dict.
#
def save_stream_part (url, filename, part,
                      inqueue, outqueue):
    stream = urllib2.urlopen (url)
    
    # non-flv: fail
    if (stream.info ().gettype () != "video/x-flv"):
        stream.close ()
        outqueue.put ({"part" : part, "status" : "Failed"})
        return
    
    outfile = open (filename + ".part" + str (part), "w")
    
    # read the one header ...
    header = read_header (stream)
    if (part == 0):
        outfile.write (header)
    
    # timestamps start at 0 no matter what so
    # need to offset timestamps
    offset = 0
    # end_time specifies the absolute end of this segment
    end_time = float ("inf")
    # duration specifies the relative end of this segment
    duration = (end_time - offset) * 1000
    
    # ... and read all the tags
    for data, _type, timestamp, body in read_tags (stream):
        do_write = True
        
        if (_type == "\x12"):
            do_write = False
            
            # the timeBase in a metadata tag indicates the ACTUAL
            # starting time of this segment - use this as offset
            timeBase = body.find ("timeBase")
            if (timeBase != -1 and timestamp == 0):
                index = timeBase + len ("timeBase") + 1
                timeBase = body[index : index + 8]
                offset = struct.unpack ("!d", timeBase)[0]
                # new offset -> re-calculate duration
                duration = (end_time - offset) * 1000
                # put timeBase - the segment before this one will
                # need to use it as the end point
                outqueue.put ({"part" : part, "offset" : offset})
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
            break
        
        # report progress
        if (duration == float ("inf") ):
            progress = 0
        else:
            progress = timestamp / duration
        outqueue.put ({"part" : part, "progress" : progress})
        
        try:
            message = inqueue.get_nowait ()
            if (message == -1):
                # we've been told to stop, so fail
                outfile.close ()
                stream.close ()
                outqueue.put ({"part" : part, "status" : "Failed"})
                return
            # otherwise, message is a new end_time -> re-calculate duration
            end_time = message
            duration = (end_time - offset) * 1000
        except Queue.Empty:
            pass
    # finished reading all that's needed - success!
    outfile.close ()
    stream.close ()
    outqueue.put ({"part" : part, "status" : "Done"})

#
#       save_stream:
#       @url_fn:        function that returns a URL for a given seek-time
#       @filename:      filename to save FLV to
#       @duration:      total duration of FLV to download
#       @parts:         number of parts in which to download FLV
#       @report_time:   interval to report stats
#
#       Downloads the FLV stream from @url_fn in several parts and save to
#       @filename. Each part has duration @duration/@parts
#       If intending to download the whole FLV, it is better to overestimate
#       @duration.
#       
#       Yields:         download stats - same as in save_stream_part()
#
def save_stream (url_fn, filename, duration, parts = 3):
    # list of [thread, queue, finished?]
    threads = list ()
    inqueue = Queue.Queue ()
    part_duration = duration / parts
    
    for i in range (parts):
        outqueue = Queue.Queue ()
        # url for this part
        part_url = url_fn (i * part_duration)
        thread = Thread (target = save_stream_part,
                         args = (part_url, filename, i,
                                 outqueue, inqueue) )
        threads.append ([thread, outqueue, False, -1])
        thread.daemon = True
        thread.start ()
    threads[-1][1].put (duration)
    
    while (True):
        message = inqueue.get ()
        part = message["part"]
        
        if ("status" in message):
            status = message["status"]
            
            # this part failed, so abort all
            if (status == "Failed"):
                for i in threads:
                    i[1].put (-1)
                yield message
                break
            
            # this part is done, check if all others are done too
            if (status == "Done"):
                threads[part][2] = True
                if (all (x[2] for x in threads) ):
                    yield message
                    break
        
        if ("offset" in message):
            # got an offset for part: this is end_time for part-1
            if (part > 0):
                threads[part - 1][1].put (message["offset"])
        yield message

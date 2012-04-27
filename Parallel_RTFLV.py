#
#       Parallel_RTFLV:
#       
#       API to download different parts of a real-time FLV
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

FAIL = -1
SUCCESS = 1
ALL_PARTS_DONE = 2

print_lock = Lock ()

#
#       print_obj:
#       @obj:           object to print
#
#       Prints @obj to stdout
#       The function uses a lock so that only one
#       print_obj() is printing at any time, otherwise
#       the output of two threads printing at the same
#       time may mix together.
#
def print_obj (obj):
    print_lock.acquire ()
    print obj
    print_lock.release ()

#
#       read_header:
#       @stream:        file-like object
#       
#       Returns:        9-byte header + 4-byte tag size (0)
#                       read from @stream, or None if stream
#                       ends prematurely
#
def read_header (stream):
    # header, tag size
    header = stream.read (9 + 4)
    if (len (header) != 13):
        return None
    return header

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
#       The function will return if it receives FAIL on @inqueue.
#       
#       The function will put a dict on outqueue with:
#               "part":         same as @part
#       and where available,
#               "offset":       timeBase value found in metadata
#               "status":       one of FAIL, SUCCESS
#               "progress":     value from 0 to 1 indicating progress
#
def save_stream_part (url, filename, part, inqueue, outqueue, debug):
    #
    #   debug_out:
    #   @obj:           object to print
    #   
    #   Prints debug msg with @obj with print_obj if @debug is True
    #
    def debug_out (obj):
        if (debug):
            print_obj ("Part " + str (part) + ": " + str (obj) )
    
    #
    #   put_message:
    #   @kwargs:        message
    #   
    #   Puts the message given in the dict @kwargs to
    #   outqueue, with part=part
    #
    def put_message (**kwargs):
        kwargs["part"] = part
        outqueue.put (kwargs)
    
    debug_out ("Started")
    debug_out ("Opening " + url)
    try:
        stream = urllib2.urlopen (url)
    except IOError as e:
        debug_out ("Failed to open " + url)
        debug_out (e)
        put_message (status = FAIL)
        return
    
    # get MIME of stream
    stream_mime = stream.info ().gettype ()
    if (stream_mime != "video/x-flv"):
        # non-flv: fail
        stream.close ()
        debug_out ("{} is {}, not FLV".format (url, stream_mime) )
        put_message (status = FAIL)
        return
    
    outfile = open (filename, "wb")
    debug_out ("Created file " + filename)
    
    # read the one header ...
    header = read_header (stream)
    if (header == None):
        debug_out ("Incomplete FLV header")
        put_message (status = FAIL)
        return
    debug_out ("Read FLV header")
    # only part 0 will write the header
    if (part == 0):
        outfile.write (header)
        debug_out ("Wrote FLV header")
    
    #offset specifies the absolute start of this part
    offset = 0
    # end_time specifies the absolute end of this part
    end_time = float ("inf")
    # duration specifies the relative end of this part
    # given by (end_time - offset) * 1000
    duration = float ("inf")
    
    tag_generator = read_tags (stream)
    
    # read first 2 tags
    try:
        # first tag - should have duration (and filesize) key
        data, _type, timestamp, body = tag_generator.next ()
        if (_type != "\x12"):
            raise StopIteration
        if (part == 0):
            full_duration = get_metadata_number (body, "duration")
            filesize = get_metadata_number (body, "filesize")
            if (full_duration == None):
                raise StopIteration
            debug_out ("Found duration ({})".format (full_duration) )
            put_message (duration = full_duration, filesize = filesize)
            outfile.write (data)
        
        # second tag - should have timeBase key
        data, _type, timestamp, body = tag_generator.next ()
        if (_type != "\x12"):
            raise StopIteration
        offset = get_metadata_number (body, "timeBase")
        if (offset == None):
            raise StopIteration
        debug_out ("Found timeBase ({})".format (offset) )
        put_message (offset = offset)
        
        data = set_tag_timestamp (data, timestamp + offset * 1000)
        outfile.write (data)
    except StopIteration:
        debug_out ("Metadata tag missing duration/timeBase key "
                   "or no metadata found")
        put_message (status = FAIL)
        return
    
    # now get end_time
    message = inqueue.get ()
    if (message == FAIL):
        # we've been told to stop, so fail
        debug_out ("Ordered to stop")
        outfile.close ()
        stream.close ()
        put_message (status = FAIL)
        return
    # otherwise, message is end_time
    end_time = message
    duration = (end_time - offset) * 1000
    
    debug_out ("Got end_time ({}), "
               "new duration is ({})".format (end_time, duration) )
    
    # ... and read all the other tags
    for data, _type, timestamp, body in tag_generator:
        do_write = True
        
        if (_type == "\x08" and (ord (body[0]) >> 4) == 10):
            # aac sequence header - only write if first part
            do_write = (body[1] != "\x00")
        elif (_type == "\x09" and (ord (body[0]) & 0xf) == 7):
            # avc sequence header - only write if first part
            do_write = (body[1] != "\x00")
        
        # write to file if first part or not sequence header
        if (do_write or part == 0):
            data = set_tag_timestamp (data, timestamp + offset * 1000)
            outfile.write (data)
        
        # reached @duration ?
        if (timestamp >= duration):
            debug_out ("Finished at {}".format (duration) )
            break
        
        # report progress for audio, video tags
        if (_type == "\x08" or _type == "\x09"):
            if (duration == float ("inf") ):
                progress = 0
            else:
                progress = timestamp / duration
            put_message (progress = progress)
        
        try:
            message = inqueue.get_nowait ()
            if (message == FAIL):
                # we've been told to stop, so fail
                debug_out ("Ordered to stop")
                outfile.close ()
                stream.close ()
                put_message (status = FAIL)
                return
        except Queue.Empty:
            pass
    # finished reading all that's needed - success!
    outfile.close ()
    stream.close ()
    debug_out ("Done")
    put_message (status = SUCCESS)

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
#       (in which case it should be threaded).
#       
#       The function will start part 0 first to obtain the full duration
#       of the video. If this fails, the function aborts.
#       Otherwise, it should receive a dict on inqueue with key "duration".
#       The function will then start all other parts.
#       Each of these parts should obtain a timeBase value in the second
#       FLV tag and put this on inqueue with the key "offset". The timeBase
#       of part X is the end_time of part X-1; the function will send
#       the value on to part X-1 via a queue (unless X=0).
#       
#       If @mainqueue != None, the function will send all messages received
#       on inqueue to through @mainqueue. They have the same format as for
#       save_stream_part(), but "status" may also be ALL_PARTS_DONE
#       
#       The function will abort if any one part fails.
#       
#       If the download was successful, the partial files are joined
#       into @filename (and then deleted).
#
def save_stream (url_fn, filename, parts = 3,
                 mainqueue = None, duration = float ("inf"), debug = False):
    # list of [thread, queue, finished?]
    #   threads[x][0] is a thread
    #   threads[x][1] is queue to communicate with thread
    #   threads[x][2] is whether the thread is done
    threads = list ()
    # queue to receive input from
    inqueue = Queue.Queue ()
    
    if (debug):
        print_obj ("Starting part 0")
    # start part 0 first to get duration
    outqueue = Queue.Queue ()
    # url for this part
    part_url = url_fn (0)
    thread = Thread (target = save_stream_part,
                     args = (part_url, filename, 0,
                             outqueue, inqueue, debug) )
    threads.append ([thread, outqueue, False])
    thread.daemon = True
    thread.start ()
    
    # expecting first message with duration; error otherwise
    message = inqueue.get ()
    if ("duration" not in message):
        if (debug):
            print_obj ("Failed to get duration. Aborting")
        if (mainqueue != None):
            mainqueue.put (FAIL)
        return
    duration = min (message["duration"], duration)
    if (mainqueue != None):
        mainqueue.put ({"duration" : duration,
                        "filesize" : message.get ("filesize")})
    if (debug):
        print_obj ("Filesize: " + str (message.get ("filesize") ) )
    
    if (debug):
        print_obj ("Starting parts 1 - " + str (parts - 1) )
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
    # last part should end at most at duration
    # add 0.5 just in case
    threads[-1][1].put (duration + 0.5)
    
    # process loop, wait for messages on inqueue
    while (True):
        message = inqueue.get ()
        part = message["part"]
        
        if ("status" in message):
            status = message["status"]
            
            # this part failed, so abort all
            if (status == FAIL):
                if (debug):
                    print_obj ("Part {} failed. "
                               "Stopping all parts".format (part) )
                for i in threads:
                    i[1].put (FAIL)
                if (mainqueue != None):
                    mainqueue.put (message)
                return
            
            # this part is done, check if all others are done too
            if (status == SUCCESS):
                threads[part][2] = True
                if (all (x[2] for x in threads) ):
                    message["status"] = ALL_PARTS_DONE
                    if (debug):
                        print_obj ("All parts done")
                    if (mainqueue != None):
                        mainqueue.put (message)
                    break
        
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
    ofile = open (filename, "ab")
    for i in range (1, parts):
        part_filename = filename + ".part" + str (i)
        
        partfile = open (part_filename, "rb")
        shutil.copyfileobj (partfile, ofile)
        partfile.close ()
        if (debug):
            print_obj ("Appended part {} : {}".format (i, part_filename) )
        
        os.remove (part_filename)
        if (debug):
            print_obj ("Deleted  part {} : {}".format (i, part_filename) )
    ofile.close ()
    if (debug):
        print_obj ("Joining done")

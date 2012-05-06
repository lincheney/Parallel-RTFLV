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
#       The use this, create a #MultiPart_Downloader, connect
#       to the desired 'signals' and call #MultiPart_Downloader.save_stream()
#       
#       #MultiPart_Downloader has a basic 'GTK-like' interface for
#       signals. Basically, you can 'connect' to them and 'emit' them.
#       Signal handlers WILL block #MultiPart_Downloader.save_stream()
#       For details on individual signals, see the documentation for
#       the #MultiPart_Downloader class.
#

import os
import shutil
import urllib2
import struct
import Queue
from threading import Thread

# possible status values
# %FAIL and %SUCCESS refer to downloading
FAIL = -1
SUCCESS = 1
JOINING_STARTED = 2
JOINING_FINISHED = 3

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
#       MultiPart_Downloader:
#       
#       Downloader for FLV-stream in multiple parts.
#       This is achieved with the save_stream() method.
#       To get debug messages, progress reports etc.,
#       you can specify a callback in the connect() method
#       for the 'signals' below.
#
class MultiPart_Downloader:
    signals = [
        #
        #       ::debug:
        #       @message:       the debug message
        #       @part:          the part for this message, or None
        #       
        #       The ::debug signal is emitted when there is a debug
        #       message available.
        #       
        #       Signal handler signature:
        #       def user_function (message, part, ...)
        #
            "debug",
        #
        #       ::got-duration:
        #       @duration:      duration
        #       
        #       Emitted when the total duration of the video is found
        #       by the first part.
        #       
        #       Signal handler signature:
        #       def user_function (duration, ...)
        #
            "got-duration",
        #
        #       ::got-filesize:
        #       @filesize:      filesize
        #       
        #       Emitted when the filesize of the video is found by
        #       the first part.
        #       
        #       Signal handler signature:
        #       def user_function (filesize, ...)
        #
            "got-filesize",
        #
        #       ::part-finished:
        #       @part:          the part that has finished
        #       
        #       Emitted when a part has finished downloading.
        #       
        #       Signal handler signature:
        #       def user_function (part, ...)
        #
            "part-finished",
        #
        #       ::part-failed:
        #       @part:          the part that failed
        #       
        #       Emitted when a part fails in downloading.
        #       
        #       Signal handler signature:
        #       def user_function (part, ...)
        #
            "part-failed",
        #
        #       ::progress:
        #       @progress:      progress of the download of @part;
        #                       from 0-1
        #       @part:          part
        #       
        #       Emitted when a part has download progress to report.
        #       
        #       Signal handler signature:
        #       def user_function (progress, part, ...)
        #
            "progress",
        #
        #       ::status-changed:
        #       @status:        status; one of FAIL, SUCCESS,
        #                       JOINING_STARTED, JOINING_FINISHED
        #       
        #       Emitted when downloading fails (FAIL), downloading has
        #       finished (SUCCESS), joining has started (JOINING_STARTED)
        #       and joining has finished (JOINING_FINISHED)
        #       
        #       Signal handler signature:
        #       def user_function (status, ...)
        #
            "status-changed",
              ]
    #
    #   __init__:
    #
    def __init__ (self):
        # signal handlers
        self.handlers = dict ()
        for i in self.signals:
            self.handlers[i] = list ()
        
        # queue receiving input from threads
        self.inqueue = Queue.Queue ()
        # list of [thread, queue, done?] for each part
        #       thread is the actual thread
        #       queue sends output to thread
        #       done indicates if the thread is finished (successfully)
        self.threads = list ()
    
    #
    #   connect:
    #   @signal_name:           name of the signal
    #   @handler:               callback to call when signal is emitted
    #   @args:                  extra arguments to pass to @handler
    #   @kwargs:                extra keyword args to pass to @handler
    #   
    #   @handler will be called (with extra args in @args and @kwargs) when
    #   the signal for @signal_name is emitted.
    #   
    #   Returns:                a handler id that can be used in disconnect()
    #
    def connect (self, signal_name, handler, *args, **kwargs):
        handler = (handler, args, kwargs)
        self.handlers[signal_name].append (handler)
        return handler
    
    #
    #   disconnect:
    #   @signal_name:           name of signal
    #   @handler_id:            handler id obtained from connect()
    #   
    #   Disconnect the handler from the signal, so the handler will no longer
    #   be called.
    #
    def disconnect (self, signal_name, handler_id):
        for index, handler in enumerate (self.handlers[signal_name]):
            # use 'is' to ensure it is an exact identity match
            if (handler is handler_id):
                self.handlers[signal_name].pop (index)
                break
    
    #
    #   emit:
    #   @signal_name:           name of signal to emit
    #   @args:                  args
    #   @kwargs:                keyword args
    #   
    #   'Emit' the signal for @signal_name. The handlers connected will
    #   each be called in order. @args and @kwargs are arguments that
    #   will be passed to EACH handler. The optional 'user_data' args
    #   for each handler are only in addition to this.
    #
    def emit (self, signal_name, *args, **kwargs):
        for function, a, kwa in self.handlers[signal_name]:
            a = args + a
            kwa.update (kwargs)
            function (*a, **kwa)
    
    #
    #   start_part_thread:
    #   @part:                  part
    #   @part_url:              url for this part
    #   @filename:              base filename
    #   
    #   Start the downloading of the part @part in a separate thread.
    #   If @part==0, the filename is @filename, otherwise it is
    #   @filename.part3 for example, if @part==3
    #
    def start_part_thread (self, part, part_url, filename):
        outqueue = Queue.Queue ()
        if (part == 0):
            part_filename = filename
        else:
            part_filename = part_filename = filename + ".part" + str (part)
        thread = Thread (target = self.save_stream_part,
            args = (part_url, part_filename, part, outqueue, self.inqueue) )
        self.threads.append ([thread, outqueue, False])
        thread.daemon = True
        thread.start ()
    
    #
    #   save_stream_part:
    #   @url:                   url to download
    #   @filename:              filename to save to
    #   @part:                  the number part this is
    #   @inqueue:               queue to receive input
    #   @outqueue:              queue to send output
    #   
    #   Downloads a part of the video stream given in @url and saves
    #   to @filename.
    #   The function will return if %FAIL is received on @inqueue.
    #   The function will NEVER emit signals (since it is intended to
    #   be threaded). Instead, it will pass them to @outqueue for
    #   emission.
    #   
    #   The messages passed to @outqueue are all dictionaries.
    #   The expected input/output of the function:
    #   
    #   1. Output a series of messages with keys "debug" and/or "filesize",
    #           followed by a message with "duration" or a final message
    #           with "status".
    #
    #   2. Wait for input on @inqueue that should either be %FAIL or the
    #           absolute end time of this part.
    #
    #   3. Output EITHER a message with "offset" OR
    #           a final message with "status".
    #
    #   4. Output a series of messages with keys "debug" and/or "progress",
    #           followed by a final message with "status".
    #
    #   You can expect any message to potentially have "status" or "debug"
    #   and a message with "status" will always be the last message.
    #   In addition to all of the above, EVERY message will have a "part"
    #   key.
    #
    def save_stream_part (self, url, filename, part, inqueue, outqueue):
        #
        #   put_message:
        #   @kwargs:        message
        #   
        #   Puts the message given in the dict @kwargs to
        #   outqueue, with part=part
        #   Convenient function to output to @outqueue
        #
        def put_message (**kwargs):
            kwargs["part"] = part
            outqueue.put (kwargs)
        
        # try to open the url
        put_message (debug = "Opening " + url)
        try:
            stream = urllib2.urlopen (url)
        except IOError as e:
            put_message (debug = "Failed to open {}: {}".format (url, e),
                         status = FAIL)
            return
        
        # get MIME of stream
        stream_mime = stream.info ().gettype ()
        if (stream_mime != "video/x-flv"):
            # non-flv: fail
            stream.close ()
            put_message (debug = "{} is {}, not FLV".format (url, stream_mime),
                         status = FAIL)
            return
        
        outfile = open (filename, "wb")
        put_message (debug = "Created file " + filename)
        
        # read the one header ...
        header = read_header (stream)
        if (header == None):
            put_message (debug = "Incomplete FLV header", status = FAIL)
            return
        put_message (debug = "Read FLV header")
        # only part 0 will write the header
        if (part == 0):
            outfile.write (header)
            put_message (debug = "Wrote FLV header")
        
        #offset specifies the absolute start of this part
        offset = 0
        # end_time specifies the absolute end of this part
        end_time = float ("inf")
        # duration specifies the relative end of this part
        # given by (end_time - offset) * 1000
        duration = float ("inf")
        
        # generator that yields (data, _type, timestamp, body)
        tag_generator = read_tags (stream)
        
        # read first 2 tags
        try:
            # first tag - should have duration (and filesize) key
            data, _type, timestamp, body = tag_generator.next ()
            # not metadata tag: fail
            if (_type != "\x12"):
                raise StopIteration
            # only bother to extract duration/filesize if part 0
            if (part == 0):
                full_duration = get_metadata_number (body, "duration")
                filesize = get_metadata_number (body, "filesize")
                # no duration found: fail
                if (full_duration == None):
                    raise StopIteration
                # output filesize if found
                if (filesize != None):
                    put_message (filesize = filesize)
                put_message (duration = full_duration)
                outfile.write (data)
            
            # second tag - should have timeBase key
            data, _type, timestamp, body = tag_generator.next ()
            # not metadata tag: fail
            if (_type != "\x12"):
                raise StopIteration
            offset = get_metadata_number (body, "timeBase")
            # no timeBase: fail
            if (offset == None):
                raise StopIteration
            put_message (debug = "Found timebase ({})".format (offset),
                    offset = offset)
            
            data = set_tag_timestamp (data, timestamp + offset * 1000)
            outfile.write (data)
        except StopIteration:
            # we ran out of tags or we explicitly raised StopIteration
            #   because of some problem
            put_message (debug = "Metadata tag missing duration/timeBase key"
                    "or no metadata found", status = FAIL)
            return
        
        # now get end_time
        message = inqueue.get ()
        if (message == FAIL):
            # we've been told to stop, so fail
            put_message (debug = "Ordered to stop")
            outfile.close ()
            stream.close ()
            put_message (status = FAIL)
            return
        # otherwise, message is end_time
        end_time = message
        duration = (end_time - offset) * 1000
        
        put_message (debug = "Got end_time ({}), "
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
                put_message (debug = "Finished at {}".format (duration) )
                break
            
            # report progress for audio, video tags
            if (_type == "\x08" or _type == "\x09"):
                if (duration == float ("inf") ):
                    progress = 0
                else:
                    progress = timestamp / duration
                put_message (progress = progress)
            
            # check if we've been told to stop
            try:
                message = inqueue.get_nowait ()
                if (message == FAIL):
                    # we've been told to stop, so fail
                    put_message (debug = "Ordered to stop")
                    outfile.close ()
                    stream.close ()
                    put_message (status = FAIL)
                    return
            except Queue.Empty:
                pass
        # finished reading all that's needed - success!
        outfile.close ()
        stream.close ()
        put_message (debug = "Done", status = SUCCESS)
    
    #
    #   stop_all_parts:
    #   
    #   Tell each of the 'part-threads' to stop and wait for them
    #   to finish.
    #
    def stop_all_parts (self):
        for thread, queue, done in self.threads:
            queue.put (FAIL)
        for thread, queue, done in self.threads:
            thread.join ()
    
    #
    #   save_stream:
    #   @url_fn:        function that returns a URL for a given seek-time
    #   @filename:      filename to save FLV to
    #   @parts:         number of parts in which to download FLV
    #   @duration:      total duration of FLV to download
    #
    #   Downloads the FLV stream from @url_fn in several parts and save to
    #   @filename.
    #   Specify @duration if not downloading full video.
    #
    #   This is the ONLY function that will emit signals (none of the threaded
    #   save_stream_part() calls will).
    #   
    #   The function will start part 0 first to obtain the full duration
    #   of the video. If this fails, the function aborts.
    #   
    #   Each of these parts should obtain a timeBase value in the second
    #   FLV tag and put this on inqueue with the key "offset". The timeBase
    #   of part X is the end_time of part X-1; the function will send
    #   the value on to part X-1 via a queue (unless X=0).
    #
    #   The function will abort if any one part fails.
    #
    #   If the download was successful, the partial files are joined
    #   into @filename (and then deleted).
    #
    def save_stream (self, url_fn, filename, parts, duration = float ("inf") ):
        # reset thread list and inqueue
        self.threads = list ()
        self.inqueue = Queue.Queue ()
        
        # start part 0 first to get duration
        self.emit ("debug", "Starting part 0", None)
        part_url = url_fn (0)
        self.start_part_thread (0, part_url, filename)
        
        # wait for a message with "duration" in it
        # if "status" is found first, fail
        # if "debug" or "filesize" is found, emit the appropriate signals
        while (True):
            message = self.inqueue.get ()
            part = message["part"]
            
            # check if there is a debug message
            # this is done first, in case there is also a status=FAIL
            if ("debug" in message):
                self.emit ("debug", message["debug"], part)
            
            # check for a status change; either way, we didn't
            #   get duration, so fail
            if ("status" in message):
                self.emit ("debug", "Failed to get duration. Aborting", None)
                self.emit ("status-changed", FAIL)
                return
            
            # check for filesize
            filesize = message.get ("filesize")
            if (filesize != None):
                self.emit ("debug",
                        "Found filesize ({})".format (filesize),
                        None)
                self.emit ("got-filesize", filesize)
            
            # check for duration; if found, we can start other threads
            if ("duration" in message):
                duration = min (message["duration"], duration)
                self.emit ("debug",
                        "Found duration ({})".format (message["duration"]),
                        None)
                self.emit ("got-duration", duration)
                break
        
        # now that we have duration, we can start all other parts
        self.emit ("debug", "Starting parts 1 - " + str (parts - 1), None)
        part_duration = duration / parts
        for i in range (1, parts):
            part_url = url_fn (i * part_duration)
            self.start_part_thread (i, part_url, filename)
        
        # last part should end at most at duration
        # add 0.5 just in case
        self.threads[-1][1].put (duration + 0.5)
        
        # process loop, wait for messages on inqueue
        while (True):
            message = self.inqueue.get ()
            part = message["part"]
            
            if ("debug" in message):
                self.emit ("debug", message["debug"], part)
            
            if ("status" in message):
                status = message["status"]
                # this part failed, so abort all
                if (status == FAIL):
                    self.emit ("part-failed", part)
                    self.emit ("status-changed", FAIL)
                    self.emit ("debug", "Part {} failed."
                               "Stopping all parts".format (part), None)
                    self.stop_all_parts ()
                    return
                
                # this part is done, check if all others are done too
                if (status == SUCCESS):
                    self.threads[part][2] = True
                    self.emit ("part-finished", part)
                    if (all (x[2] for x in self.threads) ):
                        self.emit ("status-changed", status)
                        self.emit ("debug", "All parts done", None)
                        break
            
            if ("progress" in message):
                self.emit ("progress", message["progress"], part)
            
            # got an offset for part: this is end_time for part-1
            if ("offset" in message and part > 0):
                offset = message["offset"]
                self.threads[part - 1][1].put (offset)
                self.emit ("debug",
                        "Found timebase ({})".format (offset), part)
        
        # finished downloading, start joining
        self.emit ("status-changed", JOINING_STARTED)
        self.emit ("debug", "Starting to join files", None)
        # join all files and delete partials
        # first part is contained in @filename
        # others in @filename.partX
        ofile = open (filename, "ab")
        for i in range (1, parts):
            part_filename = filename + ".part" + str (i)
            
            partfile = open (part_filename, "rb")
            shutil.copyfileobj (partfile, ofile)
            partfile.close ()
            
            self.emit ("debug",
                    "Appended part {} : {}".format (i, part_filename), None)
            
            os.remove (part_filename)
            self.emit ("debug",
                    "Deleted part {} : {}".format (i, part_filename), None)
        ofile.close ()
        # finished joining - all done
        self.emit ("status-changed", JOINING_FINISHED)
        self.emit ("debug", "Joining done", None)

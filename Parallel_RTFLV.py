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

#proxy = urllib2.ProxyHandler ({"http" : "103.4.16.203:3128"})
#opener = urllib2.build_opener (proxy)
#urllib2.install_opener (opener)

#
#       Tag:
#       
#       An FLV tag
#
class Tag:
    # possible tag types
    AUDIO = 0x8
    VIDEO = 0x9
    METADATA = 0x12
    # dummy tag type
    END = 0xff
    
    #
    #   __init__:
    #   @_type:         tag type
    #   @timestamp:     tag timestamp
    #   @body:          tag body
    #   @data:          original data of tag
    #
    def __init__ (self, _type, timestamp, body, data):
        self._type = _type
        self.timestamp = timestamp
        self.body = body
        self.data = data
    
    #
    #   is_audio_header:
    #   
    #   Returns:        True iff tag is AAC sequence header
    #
    def is_audio_hdr (self):
        return self._type == Tag.AUDIO and (ord (self.body[0]) >> 4) == 10 and ord (self.body[1]) == 0
    
    #
    #   is_video_header:
    #   
    #   Returns:        True iff tag is AVC sequence header
    #
    def is_video_hdr (self):
        return self._type == Tag.VIDEO and (ord (self.body[0]) & 0xf) == 7 and ord (self.body[1]) == 0
    
    def is_video_keyframe (self):
        return self._type == Tag.VIDEO and (ord (self.body[0]) >> 4) == 1
    
    #
    #   get_metadata_number:
    #   @metadata:      metadata block (body of FLV tag)
    #   @key:           key in metadata
    #
    #   Returns:        the 64-bit float associated with @key found in @metadata
    #                   if not found, returns None
    #
    def get_metadata_number (self, key):
        length_str = struct.pack ("!I", len (key) )[2 : ]
        index = self.body.find (length_str + key)
        if (index == -1):
            return None
        index += len (key) + 3
        return struct.unpack ("!d", self.body[index : index + 8])[0]
    
    #
    #   write_data:
    #   @fileobj:       file
    #   @offset:        offset
    #
    #   Writes the tag @data to @fileobj after offsetting the timestamp by @offset
    #
    def write_data (self, fileobj, offset):
        fileobj.write (self.data[0 : 4])
        timestamp = struct.pack ("!i", self.timestamp + offset)
        fileobj.write (timestamp[1 : ] + timestamp[0])
        fileobj.write (self.data[8 : ])

#
#       StreamPart:
#
class StreamPart:
    #
    #   __init__:
    #   @inqueue:               queue to receive input
    #   @outqueue:              queue to send output
    #   @part:                  the part number
    #   @outfile:               file object to write data to
    #   @url_fn:                function to generate URLs
    #   @numparts:              total number of parts
    #
    def __init__ (self, inqueue, outqueue, part, outfile, url_fn, numparts):
        self.inqueue = inqueue
        self.outqueue = outqueue
        
        self.part = part
        self.outfile = outfile
        self.url_fn = url_fn
        
        self.is_lastpart = (self.part == numparts - 1)
        self.is_firstpart = (self.part == 0)
        self.thread = None
        self.done = False
    
    #
    #   put_message:
    #   @kwargs:        message
    #   
    #   Puts the message given in the dict @kwargs to #StreamPart.outqueue, with part=part
    #
    def put_message (self, **kwargs):
        kwargs["part"] = self.part
        self.outqueue.put (kwargs)
    
    #
    #       read_header:
    #       @stream:        stream
    #       
    #       Returns:        9-byte header + 4-byte tag size (0) read from @stream
    #                       or None if stream ends prematurely
    #
    def read_header (self, stream):
        # header, tag size
        header = stream.read (9 + 4)
        if (len (header) != 13):
            return None
        return header

    #
    #       get_next_tag:
    #       @stream:        stream
    #
    #       Reads FLV-tags from @stream
    #
    #       Returns:        a #Tag (or None if @stream ends prematurely)
    #
    def get_next_tag (self, stream):
        # type, size, timestamp, streamid
        data = stream.read (1 + 3 + 4 + 3)
        if (len (data) != 11):
            return None
        
        _type = ord (data[0])
        size = struct.unpack ("!I", "\x00" + data[1 : 4])[0]
        ext_ts = ord (data[7]) & 0x7f
        timestamp = struct.unpack ("!i", chr (ext_ts) + data[4 : 7])[0]
        #streamid = struct.unpack ("!I", "\x00" + data[8 : 11])[0]
        
        body = stream.read (size)
        fullsize = stream.read (4)
        if (len (body) != size or len (fullsize) != 4):
            return None
        data += body + fullsize
        #fullsize = struct.unpack ("!I", fullsize)[0]
        return Tag (_type, timestamp, body, data)
    
    
    #
    #   open_stream:
    #   @url:           URL to open
    #   
    #   Opens FLV stream at @url and gets the header, first 2 metadata tags
    #           and timeBase value in second metadata tag
    #   This function is called by #StreamPart.save_stream_part_threaded()
    #   
    #   Returns:        None on failure or
    #                   (stream, header, [tag1, tag2], offset)
    #   
    def open_stream (self, url):
        # try to open the url
        self.put_message (debug = "Opening " + url)
        try:
            stream = urllib2.urlopen (url)
        except IOError as e:
            self.put_message (debug = "Failed to open {}: {}".format (url, e), status = FAIL)
            return None
        
        # stream must be FLV
        stream_mime = stream.info ().gettype ()
        if (stream_mime != "video/x-flv"):
            self.put_message (debug = "{} is {}, not FLV".format (url, stream_mime), status = FAIL)
            stream.close ()
            return None
        
        # read the one header
        header = self.read_header (stream)
        if (header == None):
            self.put_message (debug = "Incomplete FLV header", status = FAIL)
            stream.close ()
            return None
        self.put_message (debug = "Read FLV header")
        
        mtags = list ()
        # read first 2 metadata tags
        for i in range (2):
            tag = self.get_next_tag (stream)
            if (tag == None or tag._type != Tag.METADATA):
                self.put_message (debug = "Missing metadata", status = FAIL)
                stream.close ()
                return None
            mtags.append (tag)
        
        # second tag - should have timeBase key
        offset = mtags[1].get_metadata_number ("timeBase")
        if (offset == None):
            self.put_message (debug = "Metadata missing timeBase key", status = FAIL)
            return None
        offset *= 1000
        self.put_message (debug = "Found timebase ({})".format (offset), offset = offset)
        
        return stream, header, mtags, offset
    
    #
    #   save_stream_part:
    #   @start_time:            time (in secs) to start downloading from
    #
    #   Will start a thread for #StreamPart.save_stream_part_threaded()
    #
    def save_stream_part (self, start_time):
        self.thread = Thread (target = self.save_stream_part_threaded, args = (start_time, ) )
        self.thread.daemon = True
        self.thread.start ()
    
    #
    #   save_stream_part_threaded:
    #   @start_time:            time (in secs) to start downloading from
    #
    #   Downloads a part of the video stream starting at @start_time and saves
    #   to #StreamPart.outfile
    #   
    #   This function will return if %FAIL is received on #StreamPart.inqueue
    #
    #   This function will NEVER emit signals (since it is intended to be threaded)
    #   Instead, it will pass appropriate messages to #StreamPart.outqueue for emission
    #   These messages are all dictionaries
    #
    #   If this is the first part, first message contains "duration"   
    #   All parts then output a message with "offset" (absolute start time)
    #   All parts then wait for the absolute end time (or %FAIL) of this part on #StreamPart.inqueue
    #   
    #   The stream will then be downloaded and saved
    #   If the stream closes prematurely, the function tries to start downloading
    #   again from the point it left off
    #   
    #   At any point, an output message may contain "debug" or "status"
    #   If the message contains "status", it will be the final message (indicating
    #   either %SUCCESS or %FAIL)
    #   In addition to all of the above, EVERY message will have a "part" key.
    #   
    #   Tags are written in chunks: tags are queued up until a video keyframe is found,
    #   and then written. If the stream closes prematurely, queued tags are dropped
    #   and the stream restarts at the last keyframe
    #
    def save_stream_part_threaded (self, start_time):
        url = self.url_fn (start_time)
        result = self.open_stream (url)
        if (result == None):
            # couldn't open stream; fail
            return
        
        # offset - absolute start of this part
        offset = 0
        # end_time - absolute end of this part
        end_time = float ("inf")
        
        stream, header, mtags, offset = result
        real_start = offset
        
        # try statement only has a finally clause to close the stream
        try:
            # only first part will write header
            # and extract duration+filesize metadata
            if (self.is_firstpart):
                self.outfile.write (header)
                self.put_message (debug = "Wrote FLV header")
                
                full_duration = mtags[0].get_metadata_number ("duration")
                if (full_duration == None):
                    self.put_message (debug = "Metadata missing duration key", status = FAIL)
                    return
                self.put_message (filesize = mtags[0].get_metadata_number ("filesize"))
                self.put_message (duration = full_duration)
                
                mtags[0].write_data (self.outfile, 0)
                mtags[1].write_data (self.outfile, 0)
            
            # now get end_time
            message = self.inqueue.get ()
            if (message == FAIL):
                self.put_message (debug = "Ordered to stop", status = FAIL)
                return
            # otherwise, message is end_time
            end_time = message
            self.put_message (debug = "Got end_time ({})".format (end_time) )
            
            # only write headers for first part
            write_audio_hdr = self.is_firstpart
            write_video_hdr = self.is_firstpart
            # list of tags starting with a video keyframe
            tag_list = list ()
            
            # loop - keep going until WHOLE part downloaded (i.e. accounting for incomplete downloads)
            while (True):
                # relative end of this part
                duration = end_time - offset
                
                # timestamp for the last audio/video/keyframe tag received
                last_audio_t = -1
                last_video_t = -1
                last_keyframe = offset
                found_first_tag = False
                
                # loop - download + save the data
                while (True):
                    tag = self.get_next_tag (stream)
                    if (tag == None):
                        # stream closed prematurely
                        break
                    
                    # end of stream indicated with a bunch of dummy tags
                    # only interested if last part
                    if (tag.timestamp == 0 and tag._type == Tag.END and self.is_lastpart):
                        missing = duration - max (last_audio_t, last_video_t)
                        self.put_message (debug = "EOS - missing {} msecs".format (missing) )
                        break
                    
                    # downloaded as much as needed
                    if (tag.timestamp >= duration):
                        if (not self.is_lastpart and (tag.timestamp > duration or not tag.is_video_keyframe () ) ):
                            # not last part and didn't find next part's key frame
                            self.put_message (debug = "Finished but didn't end on expected keyframe")
                        break
                    
                    write_tag = False
                    if (tag.is_audio_hdr () or tag.is_video_hdr () ):
                        # write sequeunce header if not already written
                        if (write_audio_hdr and tag.is_audio_hdr () ):
                            tag.write_data (self.outfile, offset)
                            write_audio_hdr = False
                        elif (write_video_hdr and tag.is_video_hdr () ):
                            tag.write_data (self.outfile, offset)
                            write_video_hdr = False
                    else:
                        # queue tag if its seq header already written and timestamp increased
                        if (tag._type == Tag.AUDIO and not write_audio_hdr and tag.timestamp > last_audio_t):
                            last_audio_t = tag.timestamp
                            write_tag = True
                        elif (tag._type == Tag.VIDEO and not write_video_hdr and tag.timestamp > last_video_t):
                            last_video_t = tag.timestamp
                            write_tag = True
                        
                        if (write_tag):
                            # first tag - shift it back to 0
                            if (not found_first_tag):
                                found_first_tag = True
                                offset -= tag.timestamp
                                duration += tag.timestamp
                            
                            if (tag.is_video_keyframe () ):
                                # found another key frame, write the queued tags
                                for i in tag_list:
                                    i.write_data (self.outfile, offset)
                                tag_list = [tag]
                                
                                self.put_message (progress = (last_keyframe - real_start) / (end_time - real_start) )
                                last_keyframe = tag.timestamp + offset
                            else:
                                # queue the tag
                                tag_list.append (tag)
                    
                    # check if we've been ordered to stop
                    try:
                        message = self.inqueue.get_nowait ()
                        if (message == FAIL):
                            self.put_message (debug = "Ordered to stop", status = FAIL)
                            return
                    except Queue.Empty:
                        pass
                
                stream.close ()
                # timestamp of last written tag
                prev_t = max (last_audio_t, last_video_t) + offset
                
                if (tag != None):
                    # finished successfully
                    # dump remaining tags to file
                    for i in tag_list:
                        i.write_data (self.outfile, offset)
                    break
                
                # otherwise: incomplete; restart stream at last keyframe
                self.put_message (debug = "Incomplete at {}. Trying to get some more".format (last_keyframe) )
                url = self.url_fn (max (0, last_keyframe / 1000.0) )
                result = self.open_stream (url)
                if (result == None):
                    # couldn't open stream; fail
                    return
                
                stream, header, mtags, offset = result
                # new stream doesn't start at the last keyframe; fail
                if (offset != last_keyframe):
                    self.put_message (debug = "Stream starts at wrong keyframe {}".format (offset), status = FAIL)
                    return
                tag_list = list ()
            
            # finished successfully!
            self.done = True
            self.put_message (progress = 1)
            self.put_message (debug = "Finished at {}".format (prev_t) )
            self.put_message (debug = "Done", status = SUCCESS)
        finally:
            stream.close ()

#
#       MultiPart_Downloader:
#       
#       Downloader for FLV-stream in multiple parts.
#       This is achieved with the save_stream() method.
#       To get debug messages, progress reports etc.,
#       you can specify a callback in the connect() method for the 'signals' below.
#
class MultiPart_Downloader:
    signals = [
        #
        #       ::debug:
        #       @message:       the debug message
        #       @part:          the part for this message, or None
        #       
        #       The ::debug signal is emitted when there is a debug message available.
        #
            "debug",
        #
        #       ::got-duration:
        #       @duration:      duration
        #       
        #       Emitted when the total duration of the video is found by the first part.
        #
            "got-duration",
        #
        #       ::got-filesize:
        #       @filesize:      filesize
        #       
        #       Emitted when the filesize of the video is found by the first part.
        #
            "got-filesize",
        #
        #       ::part-finished:
        #       @part:          the part that has finished
        #       
        #       Emitted when a part has finished downloading.
        #
            "part-finished",
        #
        #       ::part-failed:
        #       @part:          the part that failed
        #       
        #       Emitted when a part fails in downloading.
        #
            "part-failed",
        #
        #       ::progress:
        #       @progress:      progress of the download of @part from 0-1
        #       @part:          part
        #       
        #       Emitted when a part has download progress to report.
        #
            "progress",
        #
        #       ::status-changed:
        #       @status:        status; one of %FAIL, %SUCCESS, %JOINING_STARTED, %JOINING_FINISHED
        #       
        #       Emitted when downloading fails (%FAIL), downloading has finished (%SUCCESS),
        #       joining has started (%JOINING_STARTED) and joining has finished (%JOINING_FINISHED)
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
        # function to construct URL based on seek time
        self.url_fn = lambda t: ""
        self.parts = list ()
    
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
    #   Disconnect the handler from the signal, so the handler will no longer be called.
    #
    def disconnect (self, signal_name, handler_id):
        for index, handler in enumerate (self.handlers[signal_name]):
            if (handler is handler_id):
                self.handlers[signal_name].pop (index)
                break
    
    #
    #   emit:
    #   @signal_name:           name of signal to emit
    #   @args:                  args
    #   @kwargs:                keyword args
    #   
    #   'Emit' the signal for @signal_name. The handlers connected will each be called
    #   in order. @args and @kwargs are arguments that will be passed to EACH handler.
    #   The optional 'user_data' args for each handler are only in addition to this.
    #
    def emit (self, signal_name, *args, **kwargs):
        for function, a, kwa in self.handlers[signal_name]:
            a = args + a
            kwa.update (kwargs)
            function (*a, **kwa)
    
    #
    #   start_part_thread:
    #   @part:                  part
    #   @start_time:            time at which to start this part
    #   @filename:              base filename
    #   @numparts:              total number of parts
    #   
    #   Start the downloading of the part @part in a separate thread.
    #   If @part==0, the filename is @filename, otherwise it is @filename.part3 for example, if @part==3
    #
    def start_part_thread (self, part, start_time, filename, numparts):
        outqueue = Queue.Queue ()
        if (part == 0):
            part_filename = filename
        else:
            part_filename = filename + ".part" + str (part)
        # open the file too
        outfile = open (part_filename, "wb")
        self.emit ("debug", "Created file " + part_filename, None)
        self.parts.append (StreamPart (outqueue, self.inqueue, part, outfile, self.url_fn, numparts) )
        # start the thread
        self.parts[-1].save_stream_part (start_time)
    
    #
    #   stop_all_parts:
    #   
    #   Tell each of the #StreamPart to stop; then wait for them to finish
    #
    def stop_all_parts (self):
        for i in self.parts:
            i.queue.put (FAIL)
        for i in self.parts:
            if (i.thread != None):
                i.thread.join ()
            i.outfile.close ()
    
    #
    #   save_stream:
    #   @url_fn:        function that returns a URL for a given seek-time
    #   @filename:      filename to save FLV to
    #   @parts:         number of parts in which to download FLV
    #   @duration:      total duration of FLV to download
    #
    #   Downloads the FLV stream from @url_fn in several parts and save to @filename.
    #   Specify @duration if not downloading full video.
    #
    #   This is the ONLY function that will emit signals.
    #   
    #   The function will start part 0 first to obtain the full duration of the video.
    #   If this fails, the function aborts.
    #   
    #   Each part should put a message on inqueue with the key "offset"
    #   The "offset" of part X is the end_time of part X-1 and will be sent to
    #   part X-1 via a queue (unless X==0)
    #
    #   The function will abort if any one part fails.
    #
    #   If the download was successful, the partial files are joined into @filename (and then deleted).
    #
    def save_stream (self, url_fn, filename, parts, duration = float ("inf") ):
        # reset thread list and inqueue
        self.threads = list ()
        self.inqueue = Queue.Queue ()
        self.url_fn = url_fn
        
        # start part 0 first to get duration
        self.emit ("debug", "Starting part 0", None)
        self.start_part_thread (0, 0, filename, parts)
        
        # wait for a message with "duration" in it
        while (True):
            message = self.inqueue.get ()
            part = message["part"]
            
            # check if there is a debug message
            # this is done first, in case there is also a status=%FAIL
            if ("debug" in message):
                self.emit ("debug", message["debug"], part)
            # check for a status change; either way, we didn't get duration, so fail
            if ("status" in message):
                self.emit ("debug", "Failed to get duration. Aborting", None)
                self.stop_all_parts ()
                self.emit ("status-changed", FAIL)
                return
            
            # check for filesize
            filesize = message.get ("filesize")
            if (filesize != None):
                self.emit ("debug", "Found filesize ({})".format (filesize), None)
                self.emit ("got-filesize", filesize)
            
            # check for duration; if found, we can start other threads
            if ("duration" in message):
                duration = min (message["duration"], duration)
                self.emit ("debug", "Found duration ({})".format (message["duration"]), None)
                self.emit ("got-duration", duration)
                break
        
        # now that we have duration, we can start all other parts
        self.emit ("debug", "Starting parts 1 - {}".format (parts - 1), None)
        part_duration = float (duration) / parts
        for i in range (1, parts):
            self.start_part_thread (i, i * part_duration, filename, parts)
        
        # last part should end at most at duration
        self.parts[-1].inqueue.put (duration * 1000)
        
        # process loop, wait for messages on inqueue
        while (True):
            message = self.inqueue.get ()
            part = message["part"]
            
            if ("debug" in message):
                self.emit ("debug", message["debug"], part)
            
            if ("status" in message):
                status = message["status"]
                # if this part failed, abort all
                if (status == FAIL):
                    self.parts[part].outfile.close ()
                    self.emit ("part-failed", part)
                    self.emit ("status-changed", FAIL)
                    self.emit ("debug", "Part {} failed. Stopping all parts".format (part), None)
                    self.stop_all_parts ()
                    return
                
                # this part is done, check if all others are done too
                if (status == SUCCESS):
                    self.parts[part].outfile.close ()
                    self.emit ("part-finished", part)
                    if (all (x.done for x in self.parts) ):
                        self.emit ("status-changed", status)
                        self.emit ("debug", "All parts done", None)
                        break
            
            if ("progress" in message):
                self.emit ("progress", message["progress"], part)
            
            # got an offset for part: this is end_time for part-1
            if ("offset" in message and part > 0):
                offset = message["offset"]
                self.parts[part - 1].inqueue.put (offset)
                self.emit ("debug", "Found timebase ({})".format (offset), part)
        
        # finished downloading, start joining
        self.emit ("status-changed", JOINING_STARTED)
        self.emit ("debug", "Starting to join files", None)
        # join all files and delete partials
        # first part is contained in @filename, others in @filename.partX
        with open (filename, "ab") as ofile:
            for i in range (1, parts):
                part_filename = filename + ".part" + str (i)
                
                with open (part_filename, "rb") as partfile:
                    shutil.copyfileobj (partfile, ofile)
                
                self.emit ("debug", "Appended part {} : {}".format (i, part_filename), None)
                
                os.remove (part_filename)
                self.emit ("debug", "Deleted part {} : {}".format (i, part_filename), None)
        # finished joining - all done
        self.emit ("status-changed", JOINING_FINISHED)
        self.emit ("debug", "Joining done", None)

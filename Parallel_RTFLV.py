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
#       
#       The class has the following signals:
#           "debug"             - debug messages
#           "info"              - error & more important messages
#           "got-duration"      - duration
#           "got-filesize"      - filesize
#           "part-failed"       - a part failed
#           "part-finished"     - a part finished
#           "progress"          - progress
#

import os
import errno
import shutil
import urllib2
import struct
import itertools
import functools
from collections import namedtuple
import Queue
from threading import Thread

# possible status values
# %FAIL and %SUCCESS refer to downloading
class Status:
    FAIL = -1
    SUCCESS = 1

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
    def __init__(self, _type, timestamp, body, data):
        self._type = _type
        self.timestamp = timestamp
        self.body = body
        self.data = data
    
    #
    #   is_header:
    #   
    #   Returns:        True iff tag is AAC or AVC sequence header
    #
    def is_header(self):
        if ord(self.body[1]) != 0:
            return None
        if self._type == Tag.AUDIO and (ord(self.body[0]) >> 4) == 10:
            return Tag.AUDIO
        if self._type == Tag.VIDEO and (ord(self.body[0]) & 0xf) == 7:
            return Tag.VIDEO
        return None
    
    #
    #   is_video_keyframe:
    #   
    #   Returns:        True iff tag is a video keyframe
    #
    def is_video_keyframe(self):
        return self._type == Tag.VIDEO and (ord(self.body[0]) >> 4) == 1
    
    #
    #   get_metadata_number:
    #   @metadata:      metadata block (body of FLV tag)
    #   @key:           key in metadata
    #
    #   Returns:        the 64-bit float associated with @key found in @metadata
    #                   if not found, returns None
    #
    def get_metadata_number(self, key):
        length_str = struct.pack("!I", len(key) )[2:]
        index = self.body.find(length_str + key)
        if index == -1:
            return None
        index += len(key) + 3
        return struct.unpack("!d", self.body[index:index + 8])[0]
    
    #
    #   write_data:
    #   @fileobj:       file
    #   @offset:        offset
    #
    #   Writes the tag @data to @fileobj after offsetting the timestamp by @offset
    #
    def write_data(self, fileobj, offset):
        fileobj.write(self.data[0:4])
        timestamp = struct.pack("!i", self.timestamp + offset)
        fileobj.write(timestamp[1:] + timestamp[0])
        fileobj.write(self.data[8:])

#
#   DataStream:
#   
#   A data stream keeping track of whether header for the stream
#   is written yet and the timestamp of the last tag written for this stream.
#
class DataStream:
    def __init__(self, header_written):
        self.header_written = header_written
        self.last_timestamp = -1

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
    def __init__(self, inqueue, outqueue, part, outfile, url_fn, numparts):
        self.inqueue = inqueue
        self.outqueue = outqueue
        
        self.part = part
        self.outfile = outfile
        self.url_fn = url_fn
        
        self.is_lastpart = (self.part == numparts - 1)
        self.is_firstpart = (self.part == 0)
        
        # need to write headers only if first part
        self.data_streams = {Tag.AUDIO : DataStream(not self.is_firstpart),
                            Tag.VIDEO : DataStream(not self.is_firstpart) }
        # mapping from keyframe timestamps to file positions
        self.keyframes = {}
        # offset of current stream
        self.offset = 0
        # offset of start of this part
        self.real_offset = None
        # offset from where download is resumed (or started if could not resume)
        self.start_time = None
        
        self.thread = None
        self.done = False
        self.need_start = None
        self.need_end = None
    
    #
    #   put_message:
    #   @kwargs:        message
    #   
    #   Puts the message given in the dict @kwargs to #StreamPart.outqueue, with part=part
    #
    def put_message(self, **kwargs):
        kwargs["part"] = self.part
        self.outqueue.put(kwargs)
    
    #
    #       read_header:
    #       @stream:        stream
    #       
    #       Returns:        9-byte header + 4-byte tag size (0) read from @stream
    #                       or None if stream ends prematurely
    #
    def read_header(self, stream):
        # header, tag size
        header = stream.read(9 + 4)
        if len(header) != 13:
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
    def get_next_tag(self, stream):
        # type, size, timestamp, streamid
        data = stream.read(1 + 3 + 4 + 3)
        if len(data) != 11:
            return None
        
        _type = ord(data[0])
        size = struct.unpack("!I", "\x00" + data[1:4])[0]
        ext_ts = ord(data[7]) & 0x7f
        timestamp = struct.unpack("!i", chr(ext_ts) + data[4:7])[0]
        #streamid = struct.unpack("!I", "\x00" + data[8:11])[0]
        
        body = stream.read(size)
        fullsize = stream.read(4)
        if len(body) != size or len(fullsize) != 4:
            return None
        data += body + fullsize
        #fullsize = struct.unpack("!I", fullsize)[0]
        return Tag(_type, timestamp, body, data)
    
    #
    #   restart_from_last_keyframe:
    #   
    #   Attempt to open a stream on some keyframe in self.keyframes (starting from last keyframe)
    #   
    #   Returns:        None on failure or
    #                   (stream, header, [tag1, tag2], offset)
    #
    def restart_from_last_keyframe(self):
        for kf in sorted(self.keyframes.keys(), reverse = True):
            result = self.open_stream(start = kf)
            if result is not None:
                stream, header, mtags, offset = result
                offset = round(offset)
                if offset in self.keyframes:
                    # new stream starts at a known keyframe (which may or may not be kf)
                    self.outfile.seek(self.keyframes[offset], 0)
                    return result
                # new stream doesn't start at the keyframe
                self.put_message(info = "Stream starts at unknown keyframe {}".format(offset) )
                stream.close()
        return None
    
    #
    #   open_stream:
    #   @start:         time at which to start
    #   @analyse:       whether we are analysing a previous download
    #   
    #   Gets the header, first 2 metadata tags and timeBase value in second metadata tag from stream
    #   If @analyse is false, stream is URL opened at @start
    #   Otherwise, the stream is self.outfile (and @start is ignored)
    #   
    #   If @analyse is true and it is NOT the first part, the header, metadata etc. is not retrieved
    #   All keyword arguments are optional; if @analyse is true, @start is ignored
    #   
    #   Returns:        None on failure or
    #                   (stream, header, [tag1, tag2], offset)
    #   
    def open_stream(self, start = 0, analyse = False):
        # don't do anything if analysing and not first part
        # first part needs to check for header + metadata
        if analyse and not self.is_firstpart:
            return self.outfile, None, [], None
        
        if analyse:
            stream = self.outfile
            error_key = "debug"
        else:
            error_key = "info"
            url = self.url_fn(start / 1000.0)
            # try to open the url
            self.put_message(debug = "Opening " + url)
            try:
                stream = urllib2.urlopen(url)
            except IOError as e:
                self.put_message(info = "Failed to open {}: {}".format(url, e) )
                return None
        
            # stream must be FLV
            stream_mime = stream.info().gettype()
            if stream_mime != "video/x-flv":
                self.put_message(info = "{} is {}, not FLV".format(url, stream_mime) )
                stream.close()
                return None
        
        # read the one header
        header = self.read_header(stream)
        if header is None:
            self.put_message(**{error_key : "Incomplete FLV Header"})
            if not analyse:
                stream.close()
            return None
        self.put_message(debug = "Read FLV header")
    
        # read first 2 metadata tags
        mtags = []
        for i in range(2):
            tag = self.get_next_tag(stream)
            if tag is None or tag._type != Tag.METADATA:
                self.put_message(**{error_key : "Missing metadata"})
                if not analyse:
                    stream.close()
                return None
            mtags.append(tag)
    
        # second tag - should have timeBase key
        offset = mtags[1].get_metadata_number("timeBase")
        if offset is None:
            self.put_message(**{error_key : "Metadata missing timeBase key"})
            if not analyse:
                stream.close()
            return None
        offset *= 1000
        self.put_message(debug = "Found timebase ({})".format(offset) )
        
        return stream, header, mtags, offset
    
    #
    #   read_tag_stream:
    #   @stream:            stream
    #   @end_time:          time we want to end at
    #   @analyse:           whether we are analysing a previous download
    #   
    #   Reads and yields tags from @stream; and also:
    #       Checks if @stream has legitimately ended (e.g. with @end_time) (only for @analyse = false)
    #       
    #       Drops tags if their stream header not written or timestamp has not increased
    #       
    #       Offsets first tag back to a timestamp of 0 (@analyse=false) or sets self.offset to timestamp
    #           of first tag
    #   
    #   If the @stream closes prematurely, a final None is yielded
    #   All keyword arguments are optional; if @analyse is true, @end_time is ignored
    #   
    #   Yields:             #Tag
    #
    def read_tag_stream(self, stream, end_time = 0, analyse = False):
        found_first_tag = False
        duration = int(round(end_time - self.offset) )
        while True:
            tag = self.get_next_tag(stream)
            if tag is None:
                # stream closed prematurely
                yield None
                return
            
            handled_tag = False
            if tag._type in self.data_streams:
                if tag.is_header():
                    handled_tag = not self.data_streams[tag._type].header_written
                else:
                    handled_tag = (tag.timestamp + self.offset > self.data_streams[tag._type].last_timestamp)
            
            if handled_tag and not tag.is_header() and not found_first_tag:
                # found our first (non-header) tag
                found_first_tag = True
                if analyse:
                    self.offset = tag.timestamp
                    self.real_offset = tag.timestamp
                else:
                    self.offset -= tag.timestamp
                    duration += int(tag.timestamp)
            
            # check if stream should end (and @analyse is false)
            if not analyse and not tag.is_header():
                # end of stream indicated with a bunch of dummy tags
                # only interested if last part
                if tag.timestamp == 0 and tag._type == Tag.END and self.is_lastpart:
                    missing = end_time - max(i.last_timestamp for i in self.data_streams.values() )
                    self.put_message(info = "EOS - duration was off by {} msecs".format(missing) )
                    break
                
                # downloaded as much as needed
                if tag.timestamp >= duration:
                    if (int(tag.timestamp) == duration and tag.is_video_keyframe() ) or self.is_lastpart:
                        break
                    elif tag.timestamp > duration:
                        # not last part and didn't find next part's key frame
                        self.put_message(info = "Finished but not on expected keyframe")
                        break
            
            if handled_tag:
                if not tag.is_header():
                    self.data_streams[tag._type].last_timestamp = tag.timestamp + self.offset
                yield tag
    
    #
    #   analyse:
    #   
    #   Analyse a previous (incomplete) download saved in self.outfile
    #   Basically fills in the self.keyframes dictionary
    #   If first part, will also get the video duration (if possible)
    #
    def analyse(self):
        try:
            result = self.open_stream(analyse = True)
            if result is not None:
                stream, header, mtags, offset = result
                if self.is_firstpart:
                    # get the filesize, duration
                    full_duration = mtags[0].get_metadata_number("duration")
                    if full_duration is None:
                        # no duration key; download is bad 
                        self.put_message(info = "Metadata missing duration key", status = Status.FAIL)
                        return
                    self.put_message(filesize = mtags[0].get_metadata_number("filesize") )
                    self.put_message(duration = full_duration)
                
                # fill in the self.keyframes dictionary
                for tag in self.read_tag_stream(self.outfile, analyse = True):
                    if tag is None:
                        continue
                    if tag.is_header():
                        self.data_streams[tag._type].header_written = True
                    elif tag.is_video_keyframe():
                        self.keyframes[tag.timestamp] = self.outfile.tell() - len(tag.data)
        finally:
            # seek back to start of file
            self.outfile.seek(0, 0)
    
    #
    #   save_stream_part:
    #   @resume:                    whether to resume a previous download
    #
    #   Downloads a part of the video stream starting at @start_time and saves
    #   to self.outfile
    #   
    #   This function will return if %FAIL is received on self.inqueue
    #
    #   This function will NEVER emit signals (since it is intended to be threaded)
    #   Instead, it will pass appropriate messages to self.outqueue for emission
    #   These messages are all dictionaries
    #
    #   If this is the first part, first message contains "duration"
    #   All parts then output need_start = True (wait for a start time on self.inqueue) of
    #       need_start = False (start time already obtained; self.start_time holds a valid number)
    #
    #   The stream will then be opened at self.start_time
    #   All parts then output need_end = True, and wait for end_time on self.inqueue
    #   
    #   The stream is saved and it closes prematurely, the function tries to start downloading
    #   again from the point it left off
    #   
    #   At any point, an output message may contain "debug", "info" or "status"
    #   If the message contains "status", it will be the final message (indicating
    #   either %SUCCESS or %FAIL)
    #   In addition to all of the above, EVERY message will have a "part" key.
    #   
    #   If @resume is true, will attempt to resume from a previous download.
    #   
    def save_stream_part(self, resume = False):
        if resume:
            self.analyse()
        
        # attempt to resume
        result = self.restart_from_last_keyframe()
        resume_failed = (result is None)
        
        if resume_failed:
            # no resume
            self.real_offset = None
            # first part always starts at 0
            if self.is_firstpart:
                self.start_time = 0
        else:
            # resume successful! set self.start_time
            stream, header, mtags, self.offset = result
            self.start_time = self.offset
            self.put_message(info = "Resuming from {}".format(self.offset) )
        
        # indicate we need self.start_time or self.start_time is now a number
        self.need_start = (self.start_time is None)
        self.put_message(need_start = self.need_start)
        
        if resume_failed:
            if self.need_start:
                # wait for start_time
                self.start_time = self.inqueue.get()
                if self.start_time == Status.FAIL:
                    self.put_message(debug = "Ordered to stop", status = Status.FAIL)
                    return None
                self.put_message(debug = "Got start_time ({})".format(self.start_time) )
            
            result = self.open_stream(start = self.start_time)
            if result is None:
                # couldn't open stream; fail
                self.put_message(status = Status.FAIL)
                return
            stream, header, mtags, self.offset = result
        
        if self.real_offset is None:
            self.real_offset = self.offset
        self.offset = int(round(self.offset) )
        
        # try statement only has a finally clause to close the stream
        try:
            # only first part will write header and extract duration, filesize metadata
            # if resume succeeded, its already been done
            if resume_failed and self.is_firstpart:
                self.outfile.write(header)
                self.put_message(debug = "Wrote FLV header")
                
                full_duration = mtags[0].get_metadata_number("duration")
                if full_duration is None:
                    self.put_message(info = "Metadata missing duration key", status = Status.FAIL)
                    return
                self.put_message(filesize = mtags[0].get_metadata_number("filesize") )
                self.put_message(duration = full_duration)
            
                mtags[0].write_data(self.outfile, 0)
                mtags[1].write_data(self.outfile, 0)
            
            # indicate we need an end_time
            self.need_end = True
            self.put_message(need_end = self.need_end)
            # now get end_time
            end_time = self.inqueue.get()
            if end_time == Status.FAIL:
                self.put_message(debug = "Ordered to stop", status = Status.FAIL)
                return
            self.put_message(debug = "Got end_time ({})".format(end_time) )
            
            # loop - keep going until WHOLE part downloaded (i.e. accounting for incomplete downloads)
            while True:
                # timestamp for the last audio/video/keyframe tag received
                self.data_streams[Tag.AUDIO].last_timestamp = -1
                self.data_streams[Tag.VIDEO].last_timestamp = -1
                
                # read tags from stream
                # at the end, tag is None if stream prematurely ended
                incomplete = False
                tag = None
                for tag in self.read_tag_stream(stream, end_time = end_time):
                    if tag is None:
                        incomplete = True
                        break
                    tag.write_data(self.outfile, self.offset)
                    if tag.is_header():
                        self.data_streams[tag._type].header_written = True
                    elif tag.is_video_keyframe():
                        # new keyframe
                        self.keyframes[round(tag.timestamp + self.offset)] = self.outfile.tell() - len(tag.data)
                        # report progress
                        self.put_message(progress = float(tag.timestamp + self.offset - self.real_offset) / (end_time - self.real_offset) )
                    
                    # check if we've been ordered to stop
                    try:
                        message = self.inqueue.get_nowait()
                        if message == Status.FAIL:
                            self.put_message(debug = "Ordered to stop", status = Status.FAIL)
                            return
                    except Queue.Empty:
                        pass
                
                stream.close()
                # timestamp of last written tag
                prev_t = max(i.last_timestamp for i in self.data_streams.values() )
                
                if not incomplete:
                    # finished successfully
                    break
                
                # otherwise: incomplete; restart stream at last possible keyframe
                self.put_message(info = "Incomplete at {}. Trying to get some more".format(prev_t) )
                result = self.restart_from_last_keyframe()
                if result is None:
                    # couldn't open stream; fail
                    self.put_message(status = Status.FAIL)
                    return
                stream, header, mtags, self.offset = result
            
            # finished successfully!
            self.done = True
            self.put_message(progress = 1)
            self.put_message(debug = "Finished at {}".format(prev_t), status = Status.SUCCESS)
        finally:
            stream.close()
            # remove any trailing data
            self.outfile.truncate(self.outfile.tell() )
            self.outfile.close()

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
        #       ::info:
        #       @message:       the info message
        #       @part:          the part for this message, or None
        #       
        #       The ::info signal is emitted when there is an info message available, i.e.
        #       errors or other important messages
        #
            "info",
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
              ]
    
    #
    #   __init__:
    #
    def __init__(self):
        # signal handlers
        self.handlers = {}
        for i in self.signals:
            self.handlers[i] = []
        
        # queue receiving input from threads
        self.inqueue = Queue.Queue()
        # function to construct URL based on seek time
        self.url_fn = lambda t: ""
        self.parts = []
    
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
    def connect(self, signal_name, handler, *args, **kwargs):
        handler = (handler, args, kwargs)
        self.handlers[signal_name].append(handler)
        return handler
    
    #
    #   disconnect:
    #   @signal_name:           name of signal
    #   @handler_id:            handler id obtained from connect()
    #   
    #   Disconnect the handler from the signal, so the handler will no longer be called.
    #
    def disconnect(self, signal_name, handler_id):
        for index, handler in enumerate(self.handlers[signal_name]):
            if handler is handler_id:
                self.handlers[signal_name].pop(index)
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
    def emit(self, signal_name, *args, **kwargs):
        for function, a, kwa in self.handlers[signal_name]:
            a = args + a
            kwa.update(kwargs)
            function(*a, **kwa)
    
    #
    #   lock_file:
    #   @name:          name
    #   
    #   Creates a lock file based on @name
    #   Any call to lock_file() with the same @name will then fail (since the lock already exists)
    #   even from other processes
    #   
    #   Returns:        None if failed to create lock file; or file-descriptor on success
    #
    def lock_file(self, name):
        lockname = name + ".lock"
        try:
            fd = os.open(lockname, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0666)
        except OSError as e:
            if e.errno == errno.EEXIST:
                self.emit("info", "Lock file already exists: " + lockname, None)
                return None
        else:
            # success!
            self.emit("debug", "Created lock file: " + lockname, None)
            return fd
        self.emit("info", "Could not create lock file for some reason: " + lockname, None)
        return None
    
    #
    #   unlock_file:
    #   @name:          name
    #   @fd:            file-descriptor of lock file
    #   
    #   Deletes the lock file. Calls to lock_file() can now succeed
    #
    def unlock_file(self, name, fd):
        lockname = name + ".lock"
        os.remove(lockname)
        os.close(fd)
        self.emit("debug", "Lock file removed: " + lockname, None)
    
    #
    #   start_part_thread:
    #   @part:                  part
    #   @filename:              base filename
    #   @numparts:              total number of parts
    #   
    #   Start the downloading of the part @part in a separate thread.
    #   If @part==0, the filename is @filename, otherwise it is @filename.part3 for example, if @part==3
    #
    def start_part_thread(self, part, filename, numparts, no_resume):
        outqueue = Queue.Queue()
        if part == 0:
            part_filename = filename
        else:
            part_filename = "{}.part{}".format(filename, part)
        
        # open the file
        try:
            outfile = open(part_filename, "r+b")
            resumable = not no_resume
        except IOError as e:
            if e.errno == 2:
                # file does not exist; can't resume
                outfile = open(part_filename, "wb")
                resumable = False
            else:
                self.emit("debug", "Failed to create file: {}".format(e) )
                return False
        
        self.emit("debug", "Created file " + part_filename, None)
        sp = StreamPart(outqueue, self.inqueue, part, outfile, self.url_fn, numparts)
        self.parts.append(sp)
        # start the thread
        sp.thread = Thread(target = sp.save_stream_part, kwargs = dict(resume = resumable) )
        sp.thread.daemon = True
        sp.thread.start()
        return True
    
    #
    #   stop_all_parts:
    #   
    #   Tell each of the #StreamPart to stop; then wait for them to finish
    #
    def stop_all_parts(self):
        for i in self.parts:
            i.inqueue.put(Status.FAIL)
        for i in self.parts:
            if i.thread is not None:
                i.thread.join()
            i.outfile.close()
    
    #
    #   wait_for_message:
    #   @queue:             queue
    #   
    #   Convenience function.
    #   Gets messages from @queue, performs default actions on "debug" and "info"
    #   messages and returns whatever is left.
    #   
    #   Returns:            part, message, status
    #
    def wait_for_message(self, queue):
        message = queue.get()
        part = message.pop("part")
        
        if "debug" in message:
            self.emit("debug", message.pop("debug"), part)
        if "info" in message:
            self.emit("info", message.pop("info"), part)
        
        status = message.pop("status", None)
        return part, message, status
    
    #
    #   save_stream:
    #   @url_fn:        function that returns a URL for a given seek-time
    #   @filename:      filename to save FLV to
    #   @numparts:      number of parts in which to download FLV
    #   @duration:      total duration of FLV to download
    #   @no_resume:     don't resume previous downloads
    #   @lock:          use lock file
    #
    #   Downloads the FLV stream from @url_fn in several parts and save to @filename.
    #   Specify @duration if not downloading full video.
    #
    #   This is the ONLY function that will emit signals.
    #   
    #   The function will start part 0 first to obtain the full duration of the video.
    #   If this fails, the function aborts.
    #   
    #   Each part should put a message on inqueue with the key "need_start" (= True/False)
    #   Once all parts have emitted this, those with (need_start = True) will receive a
    #   start time on their inqueue.
    #   
    #   The same will then be done with "need_end"
    #
    #   The function will abort if any one part fails.
    #
    #   If the download was successful, the partial files are joined into @filename (and then deleted).
    #
    def save_stream(self, url_fn, filename, numparts, duration = float("inf"), no_resume = False, lock = False):
        if lock:
            lock_file_fd = self.lock_file(filename)
            if lock_file_fd is None:
                return
        
        # try has finally clause to remove lock file
        try:
            # reset thread list and inqueue
            self.threads = []
            self.inqueue = Queue.Queue()
            self.url_fn = url_fn
            
            # start part 0 first to get duration
            self.emit("debug", "Starting part 0", None)
            if not self.start_part_thread(0, filename, numparts, no_resume):
                self.emit("part-failed", 0)
                return
            
            # wait for a message with "duration" in it
            while True:
                part, message, status = self.wait_for_message(self.inqueue)
                # check for a status change; either way, we didn't get duration, so fail
                if status is not None:
                    self.emit("info", "Failed to get duration. Aborting", None)
                    self.stop_all_parts()
                    return
                
                # check for filesize
                filesize = message.get("filesize")
                if filesize is not None:
                    self.emit("debug", "Found filesize ({})".format(filesize), None)
                    self.emit("got-filesize", filesize)
                
                # check for duration; if found, we can start other threads
                if "duration" in message:
                    duration = min(message["duration"], duration)
                    self.emit("debug", "Found duration ({})".format(message["duration"]), None)
                    self.emit("got-duration", duration)
                    break
            
            # now that we have duration, we can start all other parts
            self.emit("debug", "Starting parts 1 - {}".format(numparts - 1), None)
            for i in range(1, numparts):
                if not self.start_part_thread(i, filename, numparts, no_resume):
                    self.emit("part-failed", i)
                    return
            
            # process loop, wait for messages on inqueue
            while True:
                part, message, status = self.wait_for_message(self.inqueue)
                if status is not None:
                    # if this part failed, abort all
                    if status == Status.FAIL:
                        self.emit("part-failed", part)
                        self.emit("info", "Part {} failed. Stopping all parts".format(part), None)
                        self.stop_all_parts()
                        return
                    
                    # this part is done, check if all others are done too
                    if status == Status.SUCCESS:
                        self.emit("part-finished", part)
                        if all(x.done for x in self.parts):
                            self.emit("info", "All parts finished downloading", None)
                            break
                
                if "progress" in message:
                    self.emit("progress", message["progress"], part)
                
                if "need_start" in message:
                    # this part has figured out if it needs start_time
                    # check if all other parts have too
                    if all(i.need_start is not None for i in self.parts):
                        # get contiguous chunks of parts that need start_time
                        for need_start, chunk in itertools.groupby(enumerate(self.parts), lambda p: p[1].need_start):
                            if not need_start:
                                continue
                            indices, chunk = list(zip(*chunk) )
                            left = indices[0]
                            right = indices[-1] + 1
                            
                            if left == 0:
                                left_time = 0
                            else:
                                left_time = self.parts[left - 1].start_time
                            
                            if right == numparts:
                                right_time = duration * 1000
                            else:
                                right_time = self.parts[right].real_offset
                            
                            part_duration = float(right_time - left_time) / (right - left + 1)
                            # send a start time to each of them
                            for index, p in enumerate(chunk):
                                p.inqueue.put(left_time + (index + 1) * part_duration)
                                p.need_start = False
                
                if "need_end" in message:
                    # this part has figured out if it needs end_time
                    # check if all other parts have too
                    if all(i.need_end is not None for i in self.parts):
                        for i in range(1, numparts):
                            # offset of part X is end time of part X-1
                            if self.parts[i - 1].need_end:
                                self.parts[i - 1].inqueue.put(self.parts[i].real_offset)
                                self.parts[i - 1].need_end = False
                        # last part should end at most at duration
                        self.parts[-1].inqueue.put(duration * 1000)
            
            # finished downloading, start joining
            self.emit("info", "Starting to join files", None)
            # join all files and delete partials
            # first part is contained in @filename, others in @filename.partX
            with open(filename, "ab") as ofile:
                for i in range(1, numparts):
                    part_filename = "{}.part{}".format(filename, i)
                    
                    with open(part_filename, "rb") as partfile:
                        shutil.copyfileobj(partfile, ofile)
                    
                    self.emit("debug", "Appended part {} : {}".format(i, part_filename), None)
                    os.remove(part_filename)
                    self.emit("debug", "Deleted part {} : {}".format(i, part_filename), None)
            # finished joining - all done
            self.emit("info", "Joining done", None)
        finally:
            if lock:
                self.unlock_file(filename, lock_file_fd)

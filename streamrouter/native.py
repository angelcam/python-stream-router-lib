import ctypes
import ctypes.util
import logging
import threading

from ctypes import c_char_p, c_double, c_int, c_uint, c_uint16, c_uint32, c_uint64, c_void_p, c_size_t, CDLL, CFUNCTYPE

logger = logging.getLogger(__name__)
thread_local = threading.local()


class UnsupportedNativeLibrary(Exception):

    def __init__(self, lib, expected, actual):
        exp_major, exp_minor, exp_patch = expected
        act_major, act_minor, act_patch = actual

        super().__init__("Unsupported version of the native library {} {}.{}.{} < {}.{}.{}".format(
            lib,
            act_major,
            act_minor,
            act_patch,
            exp_major,
            exp_minor,
            exp_patch,
        ))


class Library:
    """
    Native library loader.
    """

    def __init__(self):
        lib_name = ctypes.util.find_library(self.library)
        if not lib_name:
            raise Exception("Unable to find native dependency \"{}\"".format(self.library))

        self.lib = CDLL(lib_name)

        self.load_symbols()

    def load_symbols(self):
        """
        A function that's called on instance initialization. It should be used
        by sub-classes to load any native symbols from the library.
        """
        pass

    def load_function(self, name, argtypes=[], restype=None):
        """
        Load a given native function and create a field of the same name.,
        """
        function = getattr(self.lib, name)
        function.argtypes = list(argtypes)
        function.restype = restype
        setattr(self, name, function)

    def load_functions(self, functions):
        """
        Load all given native functions and create fields of the same name.
        The mehtod expect an iterable of function descriptions, where function
        descriptionn is a triplet (name, argtypes, restype).
        """
        for function in functions:
            self.load_function(*function)


def get_string(native_function, *args):
    """
    Get Python string using a given native function. The initial function
    arguments are passed via *args, the native function should expect two more
    arguments - a buffer and its capacity. These two arguments will be appended
    to *args by this function and passed to the native function.
    """
    global thread_local

    if not hasattr(thread_local, 'string_buffer'):
        thread_local.string_buffer = (None, 0)

    buf, capacity = thread_local.string_buffer

    required = native_function(*args, buf, capacity)
    if required == 0:
        return None

    while required > capacity:
        capacity = required

        buf = ctypes.create_string_buffer(capacity)

        thread_local.string_buffer = (buf, capacity)

        required = native_function(*args, buf, capacity)

    data = buf.raw[:required-1]

    return data.decode('utf-8')


# cached instance of the native library
__stream_router_lib = None


def get_stream_router_lib():
    """
    Get an instance of the StreamRouterLibrary.
    """
    global __stream_router_lib

    if __stream_router_lib is None:
        __stream_router_lib = StreamRouterLibrary()

    return __stream_router_lib


class StreamRouterLibrary(Library):
    """
    Loader for libstreamrouter.
    """

    library = 'streamrouter'

    required_lib_version = (0, 14, 0)

    LOG_LEVEL_TRACE = 0
    LOG_LEVEL_DEBUG = 1
    LOG_LEVEL_INFO = 2
    LOG_LEVEL_WARN = 3
    LOG_LEVEL_ERROR = 4
    LOG_LEVEL_CRITICAL = 5

    STATUS_OK = 0
    STATUS_NEW = 1
    STATUS_CLOSED = 2
    STATUS_UNHEALTHY = -1

    CDN_REGION_NA = 0
    CDN_REGION_EU = 1

    STREAM_FORMAT_HLS = 0
    STREAM_FORMAT_MP4 = 1
    STREAM_FORMAT_MJPEG = 2
    STREAM_FORMAT_LIVE_SNAPSHOT = 3
    STREAM_FORMAT_MPEGTS = 4
    STREAM_FORMAT_MP4_CLIP = 5

    LOG_CALLBACK = CFUNCTYPE(None, c_char_p, c_size_t, c_int, c_char_p, c_void_p, c_size_t)

    SYNCHRONIZE_CALLBACK = CFUNCTYPE(None, c_char_p)

    UPDATE_CALLBACK = CFUNCTYPE(None, c_char_p)
    CHANGE_CALLBACK = CFUNCTYPE(None)
    STATUS_CALLBACK = CFUNCTYPE(None, c_int, c_int, c_char_p)

    log_level_map = {
        LOG_LEVEL_TRACE: logging.NOTSET,
        LOG_LEVEL_DEBUG: logging.DEBUG,
        LOG_LEVEL_INFO: logging.INFO,
        LOG_LEVEL_WARN: logging.WARNING,
        LOG_LEVEL_ERROR: logging.ERROR,
        LOG_LEVEL_CRITICAL: logging.CRITICAL,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def log(file, line, level, msg, kvs, count):
            file = file.decode('utf-8')
            msg = msg.decode('utf-8')

            extra = {
                'file': '%s:%d' % (file, line),
            }

            for i in range(count):
                kv = self.srl__log_message_kv_pairs__get(kvs, i)

                key = get_string(self.srl__log_message_kv_pair__get_key, kv)
                val = get_string(self.srl__log_message_kv_pair__get_value, kv)

                extra[key] = val

            level = self.log_level_map.get(level, self.LOG_LEVEL_CRITICAL)

            logger.log(level, msg, extra=extra)

        self.log_callback = self.LOG_CALLBACK(log)

        self.srl__set_log_callback(self.log_callback)

    def check_lib_version(self):
        actual = self.lib_version
        expected = self.required_lib_version

        exp_major, exp_minor, exp_patch = expected
        act_major, act_minor, act_patch = actual

        if act_major != exp_major:
            raise UnsupportedNativeLibrary(self.library, expected, actual)
        if act_major == 0 and act_minor != exp_minor:
            raise UnsupportedNativeLibrary(self.library, expected, actual)
        if act_major > 0 and act_minor < exp_minor:
            raise UnsupportedNativeLibrary(self.library, expected, actual)
        if act_minor == exp_minor and act_patch < exp_patch:
            raise UnsupportedNativeLibrary(self.library, expected, actual)

    @property
    def lib_version(self):
        major = self.srl__lib_version__major()
        minor = self.srl__lib_version__minor()
        patch = self.srl__lib_version__patch()

        return major, minor, patch

    def load_symbols(self):
        # we need to check the lib version before loading the remaining symbols
        self.load_functions((
            ('srl__lib_version__major', [], c_uint),
            ('srl__lib_version__minor', [], c_uint),
            ('srl__lib_version__patch', [], c_uint),
        ))
        self.check_lib_version()

        self.load_functions((
            ('srl__set_log_callback', [self.LOG_CALLBACK]),
            ('srl__log_message_kv_pairs__get', [c_void_p, c_size_t], c_void_p),
            ('srl__log_message_kv_pair__get_key', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__log_message_kv_pair__get_value', [c_void_p, c_char_p, c_size_t], c_size_t),

            # srl__router_cfg__XXX functions:
            ('srl__router_cfg__new', [c_char_p, c_uint16], c_void_p),
            ('srl__router_cfg__use_http', [c_void_p]),
            ('srl__router_cfg__set_streaming_server_secret', [c_void_p, c_char_p]),
            ('srl__router_cfg__set_recording_streamer_secret', [c_void_p, c_char_p]),
            ('srl__router_cfg__set_update_interval', [c_void_p, c_uint32]),
            ('srl__router_cfg__free', [c_void_p]),

            # srl__router__XXX functions:
            ('srl__router__new', [c_void_p], c_void_p),
            ('srl__router__get_status', [c_void_p], c_int),
            ('srl__router__get_error', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__router__get_consul_service_mut', [c_void_p], c_void_p),
            ('srl__router__synchronize_async', [c_void_p, self.SYNCHRONIZE_CALLBACK]),
            ('srl__router__add_update_callback', [c_void_p, self.UPDATE_CALLBACK], c_size_t),
            ('srl__router__remove_update_callback', [c_void_p, c_size_t]),
            ('srl__router__add_change_callback', [c_void_p, self.CHANGE_CALLBACK], c_size_t),
            ('srl__router__remove_change_callback', [c_void_p, c_size_t]),
            ('srl__router__assign_streaming_master_service',
                [c_void_p, c_int, c_void_p], c_void_p),
            ('srl__router__assign_streaming_edge_service', [c_void_p, c_int, c_char_p], c_void_p),
            ('srl__router__assign_arrow_asns_service', [c_void_p, c_int, c_char_p], c_void_p),
            ('srl__router__construct_edge_camera_route', [c_void_p, c_int, c_void_p], c_void_p),
            ('srl__router__construct_master_camera_route', [c_void_p, c_int, c_void_p], c_void_p),
            ('srl__router__construct_speaker_route', [c_void_p, c_int, c_void_p], c_void_p),
            ('srl__router__construct_recording_route', [c_void_p, c_int, c_char_p], c_void_p),
            ('srl__router__construct_recording_clip_route',
                [c_void_p, c_int, c_char_p], c_void_p),
            ('srl__router__construct_recording_stream_route',
                [c_void_p, c_char_p, c_char_p], c_void_p),
            ('srl__router__create_streaming_server_access_token',
                [c_void_p, c_char_p, c_uint32, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__router__create_recording_streamer_access_token',
                [c_void_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__router__free', [c_void_p]),

            # srl__consul__XXX functions:
            ('srl__consul__get_status', [c_void_p], c_int),
            ('srl__consul__get_error', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__consul__synchronize_async', [c_void_p, self.SYNCHRONIZE_CALLBACK]),
            ('srl__consul__add_update_callback', [c_void_p, self.UPDATE_CALLBACK], c_size_t),
            ('srl__consul__remove_update_callback', [c_void_p, c_size_t]),
            ('srl__consul__add_change_callback', [c_void_p, self.CHANGE_CALLBACK], c_size_t),
            ('srl__consul__remove_change_callback', [c_void_p, c_size_t]),
            ('srl__consul__get_all_streaming_edge_services', [c_void_p], c_void_p),
            ('srl__consul__get_all_streaming_master_services', [c_void_p], c_void_p),
            ('srl__consul__get_all_arrow_asns_services', [c_void_p], c_void_p),

            # srl__device__XXX functions:
            ('srl__device__new_common', [c_char_p], c_void_p),
            ('srl__device__new_arrow', [c_char_p, c_char_p], c_void_p),
            ('srl__device__new_aoc', [c_char_p], c_void_p),
            ('srl__device__free', [c_void_p]),

            # srl__camera_route__XXX functions:
            ('srl__camera_route__get_streaming_master', [c_void_p], c_void_p),
            ('srl__camera_route__get_streaming_edge', [c_void_p], c_void_p),
            ('srl__camera_route__get_url_scheme', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__camera_route__set_url_scheme', [c_void_p, c_char_p]),
            ('srl__camera_route__is_supported_format', [c_void_p, c_int], c_int),
            ('srl__camera_route__get_base_url', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__camera_route__get_hls_base_url', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__camera_route__get_url',
                [c_void_p, c_int, c_uint32, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__camera_route__free', [c_void_p]),

            # srl__speaker_route__XXX functions:
            ('srl__speaker_route__get_streaming_master', [c_void_p], c_void_p),
            ('srl__speaker_route__get_url_scheme', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__speaker_route__set_url_scheme', [c_void_p, c_char_p]),
            ('srl__speaker_route__get_base_url', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__speaker_route__get_play_url',
                [c_void_p, c_uint32, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__speaker_route__get_stop_url',
                [c_void_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__speaker_route__get_web_rtc_signaling_url',
                [c_void_p, c_uint32, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__speaker_route__free', [c_void_p]),

            # srl__recording_route__XXX functions:
            ('srl__recording_route__get_streaming_edge', [c_void_p], c_void_p),
            ('srl__recording_route__get_url_scheme', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_route__set_url_scheme', [c_void_p, c_char_p]),
            ('srl__recording_route__get_base_url', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_route__get_stream_url',
                [c_void_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_route__get_download_url',
                [c_void_p, c_uint64, c_uint64, c_char_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_route__get_snapshot_url',
                [c_void_p, c_uint64, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_route__free', [c_void_p]),

            # srl__recording_clip_route__XXX functions:
            ('srl__recording_clip_route__get_streaming_edge', [c_void_p], c_void_p),
            ('srl__recording_clip_route__get_url_scheme',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_clip_route__set_url_scheme', [c_void_p, c_char_p]),
            ('srl__recording_clip_route__get_base_url', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_clip_route__get_stream_url',
                [c_void_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_clip_route__get_download_url',
                [c_void_p, c_uint64, c_uint64, c_char_p, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_clip_route__get_snapshot_url',
                [c_void_p, c_uint64, c_uint32, c_char_p, c_size_t], c_size_t),
            ('srl__recording_clip_route__free', [c_void_p]),

            # srl__recording_stream_route__XXX functions:
            ('srl__recording_stream_route__get_streaming_edge', [c_void_p], c_void_p),
            ('srl__recording_stream_route__get_url_scheme',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__set_url_scheme', [c_void_p, c_char_p]),
            ('srl__recording_stream_route__get_base_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__get_play_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__get_pause_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__get_speed_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__get_hls_playlist_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__get_mjpeg_stream_url',
                [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__recording_stream_route__free', [c_void_p]),

            # srl__streaming_edge_service__XXX functions:
            ('srl__streaming_edge_service__get_cdn_region', [c_void_p], c_int),
            ('srl__streaming_edge_service__get_pop', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__streaming_edge_service__get_capacity', [c_void_p], c_uint32),
            ('srl__streaming_edge_service__get_load', [c_void_p], c_uint32),
            ('srl__streaming_edge_service__get_relative_load', [c_void_p], c_double),
            ('srl__streaming_edge_service__to_service', [c_void_p], c_void_p),
            ('srl__streaming_edge_service__free', [c_void_p]),

            # srl__streaming_master_service__XXX functions:
            ('srl__streaming_master_service__get_cdn_region', [c_void_p], c_int),
            ('srl__streaming_master_service__get_pop', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__streaming_master_service__get_capacity', [c_void_p], c_uint32),
            ('srl__streaming_master_service__to_service', [c_void_p], c_void_p),
            ('srl__streaming_master_service__free', [c_void_p]),

            # srl__arrow_asns_service__XXX functions:
            ('srl__arrow_asns_service__get_cdn_region', [c_void_p], c_int),
            ('srl__arrow_asns_service__get_pop', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__arrow_asns_service__get_capacity', [c_void_p], c_uint32),
            ('srl__arrow_asns_service__to_service', [c_void_p], c_void_p),
            ('srl__arrow_asns_service__free', [c_void_p]),

            # srl__service___XXX functions:
            ('srl__service__get_id', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service__get_node_id', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service__get_host', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service__get_tags', [c_void_p], c_void_p),
            ('srl__service__is_healthy', [c_void_p], c_int),
            ('srl__service__is_disabled', [c_void_p], c_int),
            ('srl__service__get_params', [c_void_p], c_void_p),
            ('srl__service__free', [c_void_p]),

            # srl__service_tags__XXX functions:
            ('srl__service_tags__current', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service_tags__next', [c_void_p]),
            ('srl__service_tags__free', [c_void_p]),

            # srl__service_params__XXX functions:
            ('srl__service_params__current_key', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service_params__current_value', [c_void_p, c_char_p, c_size_t], c_size_t),
            ('srl__service_params__next', [c_void_p]),
            ('srl__service_params__free', [c_void_p]),

            # srl__streaming_edge_services__XXX functions:
            ('srl__streaming_edge_services__next', [c_void_p], c_void_p),
            ('srl__streaming_edge_services__free', [c_void_p]),

            # srl__streaming_master_services__XXX functions:
            ('srl__streaming_master_services__next', [c_void_p], c_void_p),
            ('srl__streaming_master_services__free', [c_void_p]),

            # srl__arrow_asns_services__XXX functions:
            ('srl__arrow_asns_services__next', [c_void_p], c_void_p),
            ('srl__arrow_asns_services__free', [c_void_p]),
        ))


class NativeObject:
    """
    Abstraction over a native object. The memory will be released automatically
    by garbage collector if a free_func is given.
    """

    def __init__(self, raw_ptr, free_func=None):
        self.__raw_ptr = raw_ptr
        self.__free_func = free_func

        if free_func is None:
            self.__free_lock = None
        else:
            self.__free_lock = threading.Lock()

    def __del__(self):
        if self.__free_func is None:
            return
        if self.__raw_ptr is None:
            return

        with self.__free_lock:
            if self.__raw_ptr is not None:
                self.__free_func(self.__raw_ptr)
            self.__raw_ptr = None

    @property
    def raw_ptr(self):
        return self.__raw_ptr

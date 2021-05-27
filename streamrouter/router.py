import asyncio
import re

from contextlib import contextmanager

from .config import RouterConfig
from .consul import (
    ArrowAsnsService, Consul, RecordingStreamerService, StreamingEdgeService, StreamingMasterService,
    SynchronizationError,
)
from .native import NativeObject, get_stream_router_lib, get_string

lib = get_stream_router_lib()


@contextmanager
def native_router_config_factory(config):
    """
    A context manager that yields native router configuration object
    initialized from a given RouterConfig.
    """
    assert type(config) is RouterConfig
    assert type(config.sync_period) is int

    assert config.sync_period > 0

    host = config.consul_host.encode('utf-8')
    port = config.consul_port

    cfg = lib.srl__router_cfg__new(host, port)
    if not cfg:
        raise Exception("Unable to create a native router configuration object.")

    lib.srl__router_cfg__use_http(cfg)
    lib.srl__router_cfg__set_update_interval(cfg, config.sync_period)

    def set_secret(native_fn, secret):
        if secret is not None:
            assert type(secret) is str
            secret = secret.encode('utf-8')
            native_fn(cfg, secret)

    set_secret(lib.srl__router_cfg__set_streaming_server_secret, config.streaming_server_secret)
    set_secret(lib.srl__router_cfg__set_recording_streamer_secret, config.recording_streamer_secret)

    try:
        yield cfg
    finally:
        lib.srl__router_cfg__free(cfg)


@contextmanager
def native_device_factory(device):
    """
    A context manager that yields native device object initialized from a
    given Device.
    """
    assert type(device) is Device

    device_id = device.device_id.encode('utf-8')

    if device.arrow_uuid is None:
        arrow_uuid = None
    else:
        arrow_uuid = device.arrow_uuid.encode('utf-8')

    if device.setup == Device.COMMON:
        res = lib.srl__device__new_common(device_id)
    elif device.setup == Device.ARROW:
        res = lib.srl__device__new_arrow(device_id, arrow_uuid)
    elif device.setup == Device.AOC:
        res = lib.srl__device__new_aoc(device_id)
    else:
        raise ValueError("Unsupported device setup.")

    if not res:
        raise Exception("Unable to create a native device.")

    try:
        yield res
    finally:
        lib.srl__device__free(res)


class UnsupportedStreamFormat(Exception):

    def __init__(self, stream_format):
        self.stream_format = stream_format


class RoutingFailed(Exception):
    pass


class Device:
    """
    A device.
    """

    COMMON = 'common'
    ARROW = 'arrow'
    AOC = 'aoc'

    # camera in preview stream have id with non-numeric prefix
    re_device_id = re.compile(r"^\D*(\d+)$")
    re_arrow_uuid = re.compile(r"^([0-9a-fA-F]{8})-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$")

    def __init__(self, device_id, setup=COMMON, arrow_uuid=None):
        device_id = str(device_id)

        m_device_id = self.re_device_id.match(device_id)

        assert m_device_id
        assert setup in {self.COMMON, self.ARROW, self.AOC}
        assert setup != self.ARROW or self.re_arrow_uuid.match(arrow_uuid)

        self.device_id = device_id
        self.numeric_device_id = int(m_device_id.group(1))
        self.setup = setup
        self.arrow_uuid = arrow_uuid


class Route(NativeObject):

    free_func = None
    get_scheme_func = None
    set_scheme_func = None
    get_base_url_func = None

    def __init__(self, raw_ptr, free_raw_ptr=True, proto='https'):
        if free_raw_ptr:
            free_func = self.free_func
        else:
            free_func = None

        super().__init__(raw_ptr, free_func=free_func)

        self.proto = proto

    @property
    def proto(self):
        """
        Get URL scheme.
        """

        assert self.raw_ptr is not None

        return get_string(self.get_scheme_func, self.raw_ptr)

    @proto.setter
    def proto(self, proto):
        """
        Set URL shceme.
        """
        assert type(proto) is str

        assert self.raw_ptr is not None

        proto = proto.encode('utf-8')

        self.set_scheme_func(self.raw_ptr, proto)

    def get_service(self, native_function, service_factory):
        assert self.raw_ptr is not None

        service = native_function(self.raw_ptr)

        if not service:
            return None

        service = service_factory(service, free_raw_ptr=False)

        # NOTE: the route MUST NOT be garbage collected if the service is still
        # reachable
        service.route = self

        return service

    def get_base_url(self):
        """
        Get route base URL.
        """
        assert self.raw_ptr is not None

        return get_string(self.get_base_url_func, self.raw_ptr)


class CameraRoute(Route):
    """
    Route to a camera.
    """

    free_func = lib.srl__camera_route__free
    get_scheme_func = lib.srl__camera_route__get_url_scheme
    set_scheme_func = lib.srl__camera_route__set_url_scheme
    get_base_url_func = lib.srl__camera_route__get_base_url

    STREAM_FORMAT_HLS = "hls"
    STREAM_FORMAT_MP4 = "mp4"
    STREAM_FORMAT_MPEGTS = "mpegts"
    STREAM_FORMAT_MJPEG = "mjpeg"
    LIVE_SNAPSHOT = "jpeg"
    MP4_CLIP = "mp4-clip"

    stream_format_map = {
        STREAM_FORMAT_HLS: lib.STREAM_FORMAT_HLS,
        STREAM_FORMAT_MP4: lib.STREAM_FORMAT_MP4,
        STREAM_FORMAT_MPEGTS: lib.STREAM_FORMAT_MPEGTS,
        STREAM_FORMAT_MJPEG: lib.STREAM_FORMAT_MJPEG,
        LIVE_SNAPSHOT: lib.STREAM_FORMAT_LIVE_SNAPSHOT,
        MP4_CLIP: lib.STREAM_FORMAT_MP4_CLIP,
    }

    def get_streaming_master(self):
        """
        Get the streaming master service used in this route.
        """
        return self.get_service(lib.srl__camera_route__get_streaming_master, StreamingMasterService)

    def get_streaming_edge(self):
        """
        Get the streaming edge service used in this route.
        """
        return self.get_service(lib.srl__camera_route__get_streaming_edge, StreamingEdgeService)

    def is_supported_format(self, stream_format):
        """
        Check if a given stream format is supported by this type of route.
        """
        native_stream_format = self.stream_format_map[stream_format]

        assert self.raw_ptr is not None

        res = lib.srl__camera_route__is_supported_format(self.raw_ptr, native_stream_format)

        return bool(res)

    def get_hls_base_url(self):
        """
        Get base URL for HLS segments.
        """
        assert self.raw_ptr is not None

        return get_string(lib.srl__camera_route__get_hls_base_url, self.raw_ptr)

    def get_url(self, stream_format, ttl=None):
        """
        Get stream URL for a given format.
        """
        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        if not self.is_supported_format(stream_format):
            raise UnsupportedStreamFormat(stream_format)

        native_stream_format = self.stream_format_map[stream_format]

        return get_string(
            lib.srl__camera_route__get_url,
            self.raw_ptr,
            native_stream_format,
            ttl)

    def get_mjpeg_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_MJPEG, ttl=ttl)

    def get_hls_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_HLS, ttl=ttl)

    def get_mp4_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_MP4, ttl=ttl)

    def get_mpegts_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_MPEGTS, ttl=ttl)

    def get_snapshot_url(self, ttl=None):
        return self.get_url(self.LIVE_SNAPSHOT, ttl=ttl)

    def get_mp4_clip_url(self, ttl=None):
        return self.get_url(self.MP4_CLIP, ttl=ttl)


class SpeakerRoute(Route):
    """
    Route to a speaker.
    """

    free_func = lib.srl__speaker_route__free
    get_scheme_func = lib.srl__speaker_route__get_url_scheme
    set_scheme_func = lib.srl__speaker_route__set_url_scheme
    get_base_url_func = lib.srl__speaker_route__get_base_url

    def get_streaming_master(self):
        """
        Get the streaming master service used in this route.
        """
        return self.get_service(lib.srl__speaker_route__get_streaming_master, StreamingMasterService)

    def get_url(self, native_function, ttl=None):
        assert self.raw_ptr is not None

        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        return get_string(native_function, self.raw_ptr, ttl)

    def get_play_url(self, ttl=None):
        """
        Get stream URL for a given format.
        """
        return self.get_url(lib.srl__speaker_route__get_play_url, ttl=ttl)

    def get_web_rtc_signaling_url(self, ttl=None):
        """
        Get stream URL for a given format.
        """
        return self.get_url(lib.srl__speaker_route__get_web_rtc_signaling_url, ttl=ttl)


class RecordingRouteBase(Route):

    get_recording_streamer_func = None

    def get_recording_streamer(self):
        """
        Get the recording streamer service used in this route.
        """
        return self.get_service(self.get_recording_streamer_func, RecordingStreamerService)


class RecordingRoute(RecordingRouteBase):
    """
    Route to a recording.
    """

    free_func = lib.srl__recording_route__free
    get_scheme_func = lib.srl__recording_route__get_url_scheme
    set_scheme_func = lib.srl__recording_route__set_url_scheme
    get_base_url_func = lib.srl__recording_route__get_base_url
    get_recording_streamer_func = lib.srl__recording_route__get_recording_streamer
    get_stream_url_func = lib.srl__recording_route__get_stream_url
    get_download_url_func = lib.srl__recording_route__get_download_url
    get_snapshot_url_func = lib.srl__recording_route__get_snapshot_url

    def get_stream_url(self, ttl=None):
        """
        Get stream URL.
        """
        assert self.raw_ptr is not None

        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        return get_string(
            self.get_stream_url_func,
            self.raw_ptr,
            ttl)

    def get_download_url(self, start, end, filename=None, ttl=None):
        """
        Get download URL.
        """
        assert self.raw_ptr is not None

        if ttl is None:
            ttl = 0
        if filename is not None:
            filename = filename.encode('utf-8')

        assert type(ttl) is int

        start = int(start.timestamp() * 1000000)
        end = int(end.timestamp() * 1000000)

        return get_string(
            self.get_download_url_func,
            self.raw_ptr,
            start,
            end,
            filename,
            ttl)

    def get_snapshot_url(self, at, ttl=None):
        """
        Get snapshot URL.
        """
        assert self.raw_ptr is not None

        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        at = int(at.timestamp() * 1000000)

        return get_string(
            self.get_snapshot_url_func,
            self.raw_ptr,
            at,
            ttl)


class RecordingClipRoute(RecordingRoute):
    """
    Route to a recording clip.
    """

    free_func = lib.srl__recording_clip_route__free
    get_scheme_func = lib.srl__recording_clip_route__get_url_scheme
    set_scheme_func = lib.srl__recording_clip_route__set_url_scheme
    get_base_url_func = lib.srl__recording_clip_route__get_base_url
    get_recording_streamer_func = lib.srl__recording_clip_route__get_recording_streamer
    get_stream_url_func = lib.srl__recording_clip_route__get_stream_url
    get_download_url_func = lib.srl__recording_clip_route__get_download_url
    get_snapshot_url_func = lib.srl__recording_clip_route__get_snapshot_url


class RecordingStreamRoute(RecordingRouteBase):
    """
    Route to a recoding stream.
    """

    free_func = lib.srl__recording_stream_route__free
    get_scheme_func = lib.srl__recording_stream_route__get_url_scheme
    set_scheme_func = lib.srl__recording_stream_route__set_url_scheme
    get_base_url_func = lib.srl__recording_stream_route__get_base_url
    get_recording_streamer_func = lib.srl__recording_stream_route__get_recording_streamer

    def get_url(self, native_function):
        """
        Get URL using a given native function.
        """
        assert self.raw_ptr is not None

        return get_string(native_function, self.raw_ptr)

    def get_play_url(self):
        return self.get_url(lib.srl__recording_stream_route__get_play_url)

    def get_pause_url(self):
        return self.get_url(lib.srl__recording_stream_route__get_pause_url)

    def get_speed_url(self):
        return self.get_url(lib.srl__recording_stream_route__get_speed_url)

    def get_hls_playlist_url(self):
        return self.get_url(lib.srl__recording_stream_route__get_hls_playlist_url)

    def get_mjpeg_stream_url(self):
        return self.get_url(lib.srl__recording_stream_route__get_mjpeg_stream_url)


class StreamRouter(NativeObject):

    CDN_REGION_EU = 'eu'
    CDN_REGION_NA = 'na'

    cdn_region_map = {
        CDN_REGION_EU: lib.CDN_REGION_EU,
        CDN_REGION_NA: lib.CDN_REGION_NA,
    }

    def __init__(self, config, loop=None):
        self.config = config

        with native_router_config_factory(config) as cfg:
            router = lib.srl__router__new(cfg)
            if not router:
                raise Exception("Unable to create a native stream router object.")

            super().__init__(router, free_func=lib.srl__router__free)

        self.event_loop = loop or asyncio.get_event_loop()

        self.update_callbacks = {}
        self.change_callbacks = {}

        consul = lib.srl__router__get_consul_service_mut(self.raw_ptr)

        self.consul = Consul(consul, loop=loop)

        # NOTE: the router MUST NOT be garbage collected if the consul service
        # is still reachable
        self.consul.router = self

    def __del__(self):
        consul = self.consul

        # free the Consul service reference first
        if consul is not None:
            consul.__del__()

        # and then the Router
        super().__del__()

    async def sync(self):
        """
        Synchronize the router with the remote Consul API.
        """
        fut = self.event_loop.create_future()

        def set_result_threadsafe(res):
            self.event_loop.call_soon_threadsafe(lambda: fut.set_result(res))

        def set_exception_threadsafe(exc):
            self.event_loop.call_soon_threadsafe(lambda: fut.set_exception(exc))

        def callback(err):
            if err is None:
                set_result_threadsafe(None)
            else:
                err = err.decode('utf-8')
                exc = SynchronizationError(err)
                set_exception_threadsafe(exc)

        ncb = lib.SYNCHRONIZE_CALLBACK(callback)

        assert self.raw_ptr is not None

        lib.srl__router__synchronize_async(self.raw_ptr, ncb)

        await fut

    def assign_streaming_master_service(self, cdn_region, camera):
        """
        Assign streaming master service from a given region for a given resource.
        """
        assert type(cdn_region) is str
        assert type(camera) is Device

        cdn_region = self.cdn_region_map[cdn_region]

        assert self.raw_ptr is not None

        with native_device_factory(camera) as c:
            svc = lib.srl__router__assign_streaming_master_service(self.raw_ptr, cdn_region, c)
            if not svc:
                return None

            return StreamingMasterService(svc)

    def assign_streaming_edge_service(self, cdn_region, pop=None):
        """
        Assign streaming edge service from a given region and POP (if given).
        """
        assert type(cdn_region) is str
        assert pop is None or type(pop) is str

        cdn_region = self.cdn_region_map[cdn_region]

        if pop is not None:
            pop = pop.encode('utf-8')

        assert self.raw_ptr is not None

        svc = lib.srl__router__assign_streaming_edge_service(self.raw_ptr, cdn_region, pop)
        if not svc:
            return None

        return StreamingEdgeService(svc)

    def assign_arrow_asns_service(self, cdn_region, arrow_uuid):
        """
        Assign Arrow ASNS service from a given region for a given Arrow UUID.
        """
        assert type(cdn_region) is str
        assert type(arrow_uuid) is str

        m_arrow_uuid = Device.re_arrow_uuid.match(arrow_uuid)

        assert m_arrow_uuid

        cdn_region = self.cdn_region_map[cdn_region]

        arrow_uuid = arrow_uuid.encode('utf-8')

        assert self.raw_ptr is not None

        svc = lib.srl__router__assign_arrow_asns_service(self.raw_ptr, cdn_region, arrow_uuid)
        if not svc:
            return None

        return ArrowAsnsService(svc)

    def assign_recording_streamer_service(self, aws_region):
        """
        Assign recording streamer service from a given AWS region.
        """
        assert type(aws_region) is str

        aws_region = aws_region.encode('utf-8')

        assert self.raw_ptr is not None

        svc = lib.srl__router__assign_recording_streamer_service(self.raw_ptr, aws_region)
        if not svc:
            return None

        return RecordingStreamerService(svc)

    def construct_master_camera_route(self, cdn_region, camera):
        """
        Construct master camera route.
        """
        return self.construct_camera_route(
            lib.srl__router__construct_master_camera_route,
            CameraRoute,
            cdn_region,
            camera)

    def construct_edge_camera_route(self, cdn_region, camera):
        """
        Construct edge camera route.
        """
        return self.construct_camera_route(
            lib.srl__router__construct_edge_camera_route,
            CameraRoute,
            cdn_region,
            camera)

    def construct_camera_route(self, native_function, route_factory, cdn_region, camera):
        assert type(cdn_region) is str
        assert type(camera) is Device

        cdn_region = self.cdn_region_map[cdn_region]

        assert self.raw_ptr is not None

        with native_device_factory(camera) as c:
            route = native_function(self.raw_ptr, cdn_region, c)
            if not route:
                raise RoutingFailed("route cannot be constructed")

            return route_factory(route, proto=self.config.stream_proto)

    def construct_speaker_route(self, cdn_region, speaker):
        assert type(cdn_region) is str
        assert type(speaker) is Device

        cdn_region = self.cdn_region_map[cdn_region]

        assert self.raw_ptr is not None

        with native_device_factory(speaker) as s:
            route = lib.srl__router__construct_speaker_route(self.raw_ptr, cdn_region, s)
            if not route:
                raise RoutingFailed("route cannot be constructed")

            return SpeakerRoute(route, proto=self.config.stream_proto)

    def construct_recording_route(self, aws_region, recording_id):
        assert type(aws_region) is str
        assert type(recording_id) is str

        aws_region = aws_region.encode('utf-8')
        recording_id = recording_id.encode('utf-8')

        return self.construct_recording_streamer_route(
            lib.srl__router__construct_recording_route,
            RecordingRoute,
            aws_region,
            recording_id)

    def construct_recording_clip_route(self, aws_region, clip_id):
        assert type(aws_region) is str
        assert type(clip_id) is str

        aws_region = aws_region.encode('utf-8')
        clip_id = clip_id.encode('utf-8')

        return self.construct_recording_streamer_route(
            lib.srl__router__construct_recording_clip_route,
            RecordingClipRoute,
            aws_region,
            clip_id)

    def construct_recording_stream_route(self, service_id, stream_id):
        assert type(service_id) is str
        assert type(stream_id) is str

        service_id = service_id.encode('utf-8')
        stream_id = stream_id.encode('utf-8')

        return self.construct_recording_streamer_route(
            lib.srl__router__construct_recording_stream_route,
            RecordingStreamRoute,
            service_id,
            stream_id)

    def construct_recording_streamer_route(self, native_function, route_factory, *args):
        assert self.raw_ptr is not None

        route = native_function(self.raw_ptr, *args)
        if not route:
            raise RoutingFailed("route cannot be constructed")

        return route_factory(route, proto=self.config.stream_proto)

    def get_streaming_server_access_token(self, device_id, ttl=None):
        """
        Get streaming server access token for a given device ID.
        """
        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        device_id = str(device_id)
        device_id = device_id.encode('utf-8')

        assert self.raw_ptr is not None

        return get_string(
            lib.srl__router__create_streaming_server_access_token,
            self.raw_ptr,
            device_id,
            ttl)

    def get_recording_streamer_access_token(self, ttl=None):
        """
        Get recording streamer access token.
        """
        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        assert self.raw_ptr is not None

        return get_string(
            lib.srl__router__create_recording_streamer_access_token,
            self.raw_ptr,
            ttl)

    def add_update_callback(self, cb):
        """
        Add a given update callback. The callback will be invoked whenever
        the router gets updated.
        """
        def callback(err):
            cb()

        ncb = lib.UPDATE_CALLBACK(callback)

        assert self.raw_ptr is not None

        token = lib.srl__router__add_update_callback(self.raw_ptr, ncb)

        self.update_callbacks[token] = ncb

        return token

    def remove_update_callback(self, token):
        """
        Remove an update callback corresponding to a given token.
        """
        assert self.raw_ptr is not None

        lib.srl__router__remove_update_callback(self.raw_ptr, token)

        self.update_callbacks.pop(token, None)

    def add_routing_changed_callback(self, cb):
        """
        Add a given routing-changed callback. The callback will be invoked
        whenever the routing information changes.
        """
        ncb = lib.CHANGE_CALLBACK(cb)

        assert self.raw_ptr is not None

        token = lib.srl__router__add_change_callback(self.raw_ptr, ncb)

        self.change_callbacks[token] = ncb

        return token

    def remove_routing_changed_callback(self, token):
        """
        Remove a routing-changed callback corresponding to a given token.
        """
        assert self.raw_ptr is not None

        lib.srl__router__remove_change_callback(self.raw_ptr, token)

        self.change_callbacks.pop(token, None)

    def is_healthy(self):
        """
        Check if the router is healthy.
        """
        assert self.raw_ptr is not None

        return lib.srl__router__get_status(self.raw_ptr) == lib.STATUS_OK

    def exception(self):
        """
        Get the last synchronization exception.
        """
        assert self.raw_ptr is not None

        err = get_string(lib.srl__router__get_error, self.raw_ptr)
        if err is None:
            return None

        return SynchronizationError(err)

    def close(self):
        """
        Close the router. Stop all background jobs and release all resources.
        """
        self.__del__()

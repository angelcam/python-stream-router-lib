import asyncio
import re

from contextlib import contextmanager

from .config import RouterConfig
from .consul import (
    ArrowAsnsService, Consul, StreamingEdgeService, StreamingMasterService, SynchronizationError,
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

    secret = config.secret.encode('utf-8')

    lib.srl__router_cfg__use_http(cfg)
    lib.srl__router_cfg__set_secret(cfg, secret)
    lib.srl__router_cfg__set_update_interval(cfg, config.sync_period)

    try:
        yield cfg
    finally:
        lib.srl__router_cfg__free(cfg)


@contextmanager
def native_resource_factory(resource):
    """
    A context manager that yields native resource object initialized from a
    given Resource.
    """
    assert type(resource) is Resource

    camera_id = resource.camera_id.encode('utf-8')

    if resource.arrow_uuid is None:
        arrow_uuid = None
    else:
        arrow_uuid = resource.arrow_uuid.encode('utf-8')

    if resource.setup == Resource.COMMON:
        res = lib.srl__resource__new_common(camera_id)
    elif resource.setup == Resource.ARROW:
        res = lib.srl__resource__new_arrow(camera_id, arrow_uuid)
    elif resource.setup == Resource.AOC:
        res = lib.srl__resource__new_aoc(camera_id)
    else:
        raise ValueError("Unsupported camera setup.")

    if not res:
        raise Exception("Unable to create a native resource.")

    try:
        yield res
    finally:
        lib.srl__resource__free(res)


class UnsupportedStreamFormat(Exception):

    def __init__(self, stream_format):
        self.stream_format = stream_format


class RoutingFailed(Exception):
    pass


class Resource:
    """
    Routing resource (i.e. a camera).
    """

    COMMON = 'common'
    ARROW = 'arrow'
    AOC = 'aoc'

    # camera in preview stream have id with non-numeric prefix
    re_camera_id = re.compile(r"^\D*(\d+)$")
    re_arrow_uuid = re.compile(r"^([0-9a-fA-F]{8})-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$")

    def __init__(self, camera_id, setup=COMMON, arrow_uuid=None):
        camera_id = str(camera_id)

        m_camera_id = self.re_camera_id.match(camera_id)

        assert m_camera_id
        assert setup in {self.COMMON, self.ARROW, self.AOC}
        assert setup != self.ARROW or self.re_arrow_uuid.match(arrow_uuid)

        self.camera_id = camera_id
        self.numeric_camera_id = int(m_camera_id.group(1))
        self.setup = setup
        self.arrow_uuid = arrow_uuid


class Route(NativeObject):
    """
    Common base class for various types of routes.
    """

    STREAM_FORMAT_HLS = "hls"
    STREAM_FORMAT_MP4 = "mp4"
    STREAM_FORMAT_MJPEG = "mjpeg"
    LIVE_SNAPSHOT = "jpeg"

    free_func = None
    to_route_func = None

    def __init__(self, raw_ptr, free_raw_ptr=True, proto='https'):
        if free_raw_ptr:
            free_func = self.free_func
        else:
            free_func = None

        super().__init__(raw_ptr, free_func=free_func)

        route = self.to_route_func(raw_ptr)

        self.proto = proto
        self.native_route = NativeRoute(route, proto=proto)

    def __del__(self):
        # free the casted reference first
        self.native_route.__del__()

        # and then the original object
        super().__del__()

    def is_supported_format(self, stream_format):
        """
        Check if a given stream format is supported by this type of route.
        """
        return self.native_route.is_supported_format(stream_format)

    def get_url(self, stream_format, ttl=None):
        """
        Get stream URL for a given format.
        """
        return self.native_route.get_url(stream_format, ttl=ttl)

    def get_mjpeg_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_MJPEG, ttl=ttl)

    def get_hls_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_HLS, ttl=ttl)

    def get_mp4_url(self, ttl=None):
        return self.get_url(self.STREAM_FORMAT_MP4, ttl=ttl)

    def get_snapshot_url(self, ttl=None):
        return self.get_url(self.LIVE_SNAPSHOT, ttl=ttl)


class NativeRoute(NativeObject):
    """
    A wrapper around a native route object.
    """

    stream_format_map = {
        Route.STREAM_FORMAT_HLS: lib.STREAM_FORMAT_HLS,
        Route.STREAM_FORMAT_MP4: lib.STREAM_FORMAT_MP4,
        Route.STREAM_FORMAT_MJPEG: lib.STREAM_FORMAT_MJPEG,
        Route.LIVE_SNAPSHOT: lib.STREAM_FORMAT_LIVE_SNAPSHOT,
    }

    def __init__(self, raw_ptr, free_raw_ptr=True, proto='https'):
        if free_raw_ptr:
            free_func = lib.srl__route__free
        else:
            free_func = None

        super().__init__(raw_ptr, free_func=free_func)

        self.proto = proto

    def is_supported_format(self, stream_format):
        """
        Check if a given stream format is supported by this type of route.
        """
        native_stream_format = self.stream_format_map[stream_format]

        assert self.raw_ptr is not None

        res = lib.srl__route__is_supported_format(self.raw_ptr, native_stream_format)

        return bool(res)

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

        scheme = self.proto.encode('utf-8')

        return get_string(
            lib.srl__route__get_url_with_custom_scheme,
            self.raw_ptr,
            scheme,
            native_stream_format,
            ttl)


class EdgeRoute(Route):
    """
    Edge stream route.
    """

    free_func = lib.srl__edge_route__free
    to_route_func = lib.srl__edge_route__to_route

    def get_service(self, native_function, service_factory):
        assert self.raw_ptr is not None

        service = native_function(self.raw_ptr)

        service = service_factory(service, free_raw_ptr=False)

        # NOTE: the route MUST NOT be garbage collected if the service is still
        # reachable
        service.route = self

        return service

    def get_streaming_master_service(self):
        """
        Get the streaming master service used in this route.
        """
        return self.get_service(lib.srl__edge_route__get_streaming_master_service, StreamingMasterService)

    def get_streaming_edge_service(self):
        """
        Get the streaming edge service used in this route.
        """
        return self.get_service(lib.srl__edge_route__get_streaming_edge_service, StreamingEdgeService)

    def get_hls_base_url(self):
        """
        Get base URL for HLS segments.
        """
        assert self.raw_ptr is not None

        scheme = self.proto.encode('utf-8')

        return get_string(
            lib.srl__edge_route__get_hls_base_url_with_custom_scheme,
            self.raw_ptr,
            scheme)


class MasterRoute(Route):
    """
    Master stream route.
    """

    free_func = lib.srl__master_route__free
    to_route_func = lib.srl__master_route__to_route

    def get_base_url(self):
        """
        Get base URL for various types of streams.
        """
        assert self.raw_ptr is not None

        scheme = self.proto.encode('utf-8')

        return get_string(
            lib.srl__master_route__get_base_url_with_custom_scheme,
            self.raw_ptr,
            scheme)

    def get_hls_base_url(self):
        """
        Get base URL for HLS segments.
        """
        assert self.raw_ptr is not None

        scheme = self.proto.encode('utf-8')

        return get_string(
            lib.srl__master_route__get_hls_base_url_with_custom_scheme,
            self.raw_ptr,
            scheme)

    def get_service(self):
        """
        Get the streaming master service used in this route.
        """
        assert self.raw_ptr is not None

        service = lib.srl__master_route__get_service(self.raw_ptr)

        service = StreamingMasterService(service, free_raw_ptr=False)

        # NOTE: the route MUST NOT be garbage collected if the service is still
        # reachable
        service.route = self

        return service


class StreamRouter(NativeObject):

    REGION_EU = 'eu'
    REGION_NA = 'na'

    region_map = {
        REGION_EU: lib.REGION_EU,
        REGION_NA: lib.REGION_NA,
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
        # free the Consul service reference first
        self.consul.__del__()

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

    def assign_streaming_master_service(self, region, resource):
        """
        Assign streaming master service from a given region for a given resource.
        """
        assert type(region) is str
        assert type(resource) is Resource

        region = self.region_map[region]

        assert self.raw_ptr is not None

        with native_resource_factory(resource) as res:
            svc = lib.srl__router__assign_streaming_master_service(self.raw_ptr, region, res)
            if not svc:
                return None

            return StreamingMasterService(svc)

    def assign_streaming_edge_service(self, region, pop=None):
        """
        Assign streaming edge service from a given region and POP (if given).
        """
        assert type(region) is str
        assert pop is None or type(pop) is str

        region = self.region_map[region]

        if pop is not None:
            pop = pop.encode('utf-8')

        assert self.raw_ptr is not None

        svc = lib.srl__router__assign_streaming_edge_service(self.raw_ptr, region, pop)
        if not svc:
            return None

        return StreamingEdgeService(svc)

    def assign_arrow_asns_service(self, region, arrow_uuid):
        """
        Assign Arrow ASNS service from a given region for a given Arrow UUID.
        """
        assert type(region) is str
        assert type(arrow_uuid) is str

        m_arrow_uuid = Resource.re_arrow_uuid.match(arrow_uuid)

        assert m_arrow_uuid

        region = self.region_map[region]

        arrow_uuid = arrow_uuid.encode('utf-8')

        assert self.raw_ptr is not None

        svc = lib.srl__router__assign_arrow_asns_service(self.raw_ptr, region, arrow_uuid)
        if not svc:
            return None

        return ArrowAsnsService(svc)

    def construct_master_route(self, region, resource):
        """
        Construct master route.
        """
        return self.construct_route(
            lib.srl__router__construct_master_route,
            MasterRoute,
            region,
            resource)

    def construct_edge_route(self, region, resource):
        """
        Construct edge route.
        """
        return self.construct_route(
            lib.srl__router__construct_edge_route,
            EdgeRoute,
            region,
            resource)

    def construct_route(self, native_function, route_factory, region, resource):
        assert type(region) is str
        assert type(resource) is Resource

        region = self.region_map[region]

        assert self.raw_ptr is not None

        with native_resource_factory(resource) as res:
            route = native_function(self.raw_ptr, region, res)
            if not route:
                raise RoutingFailed("route cannot be constructed")

            return route_factory(route, proto=self.config.stream_proto)

    def get_stream_access_token(self, camera_id, stream_format, ttl=None):
        """
        Get stream access token for a given camera ID.
        """
        if ttl is None:
            ttl = 0

        assert type(ttl) is int

        native_stream_format = NativeRoute.stream_format_map[stream_format]

        camera_id = str(camera_id)
        camera_id = camera_id.encode('utf-8')

        assert self.raw_ptr is not None

        return get_string(
            lib.srl__router__create_stream_access_token,
            self.raw_ptr,
            camera_id,
            native_stream_format,
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

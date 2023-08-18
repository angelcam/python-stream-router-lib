import asyncio

from .native import NativeObject, get_stream_router_lib, get_string

lib = get_stream_router_lib()


class cached_property:
    """
    Custom decorator for cached properties.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            raise AttributeError("not a class attribute")

        value = self.func(obj)

        # this replaces the cached property decorator in the owner __dict__
        # with the computed value
        obj.__dict__[self.func.__name__] = value

        return value


class SynchronizationError(Exception):
    pass


class Service(NativeObject):
    """
    Common base class for various types of services.
    """

    free_func = None
    to_service_func = None

    def __init__(self, raw_ptr, free_raw_ptr=True):
        if free_raw_ptr:
            free_func = self.free_func
        else:
            free_func = None

        super().__init__(raw_ptr, free_func=free_func)

        service = self.to_service_func(raw_ptr)

        self.native_service = NativeService(service)

    def __del__(self):
        # free the casted reference first
        self.native_service.__del__()

        # and then the original object
        super().__del__()

    @property
    def id(self):
        return self.native_service.id

    @property
    def node_id(self):
        return self.native_service.node_id

    @property
    def host(self):
        return self.native_service.host

    @property
    def tags(self):
        return self.native_service.tags

    def has_tag(self, tag):
        return self.native_service.has_tag(tag)

    @property
    def healthy(self):
        return self.native_service.healthy

    @property
    def disabled(self):
        return self.native_service.disabled

    @property
    def params(self):
        return self.native_service.params

    def get_param(self, name):
        return self.native_service.get_param(name)

    def __str__(self):
        return self.id


class NativeService(NativeObject):
    """
    A wrapper around a native service object.
    """

    def __init__(self, raw_ptr, free_raw_ptr=True):
        if free_raw_ptr:
            free_func = lib.srl__service__free
        else:
            free_func = None

        super().__init__(raw_ptr, free_func=free_func)

    @cached_property
    def id(self):
        assert self.raw_ptr is not None

        return get_string(lib.srl__service__get_id, self.raw_ptr)

    @cached_property
    def node_id(self):
        assert self.raw_ptr is not None

        return get_string(lib.srl__service__get_node_id, self.raw_ptr)

    @cached_property
    def host(self):
        assert self.raw_ptr is not None

        return get_string(lib.srl__service__get_host, self.raw_ptr)

    @cached_property
    def tags(self):
        assert self.raw_ptr is not None

        tags = set()

        iterator = lib.srl__service__get_tags(self.raw_ptr)

        try:
            while True:
                tag = get_string(lib.srl__service_tags__current, iterator)
                if tag is None:
                    break

                tags.add(tag)

                lib.srl__service_tags__next(iterator)
        finally:
            lib.srl__service_tags__free(iterator)

        return tags

    def has_tag(self, tag):
        return tag in self.tags

    @cached_property
    def healthy(self):
        assert self.raw_ptr is not None

        return bool(lib.srl__service__is_healthy(self.raw_ptr))

    @cached_property
    def disabled(self):
        assert self.raw_ptr is not None

        return bool(lib.srl__service__is_disabled(self.raw_ptr))

    @cached_property
    def params(self):
        assert self.raw_ptr is not None

        params = {}

        iterator = lib.srl__service__get_params(self.raw_ptr)

        try:
            while True:
                key = get_string(lib.srl__service_params__current_key, iterator)
                val = get_string(lib.srl__service_params__current_value, iterator)
                if key is None:
                    break

                params[key] = val

                lib.srl__service_params__next(iterator)
        finally:
            lib.srl__service_params__free(iterator)

        return params

    def get_param(self, name):
        return self.params.get(name)


class CdnServiceMixin:

    get_cdn_region_func = None
    get_pop_func = None

    @cached_property
    def cdn_region(self):
        assert self.raw_ptr is not None

        region = self.get_cdn_region_func(self.raw_ptr)
        if region == lib.CDN_REGION_EU:
            return 'eu'
        elif region == lib.CDN_REGION_NA:
            return 'na'
        else:
            return None

    @cached_property
    def pop(self):
        assert self.raw_ptr is not None

        return get_string(self.get_pop_func, self.raw_ptr)


class ServiceCapacityMixin:

    get_capacity_func = None

    @cached_property
    def capacity(self):
        assert self.raw_ptr is not None

        return self.get_capacity_func(self.raw_ptr)


class ServiceLoadMixin:

    get_load_func = None
    get_relative_load_func = None

    @cached_property
    def load(self):
        assert self.raw_ptr is not None

        return self.get_load_func(self.raw_ptr)

    @cached_property
    def relative_load(self):
        assert self.raw_ptr is not None

        return self.get_relative_load_func(self.raw_ptr)


class StreamingEdgeService(CdnServiceMixin, ServiceLoadMixin, ServiceCapacityMixin, Service):
    """
    Streaming edge service.
    """

    free_func = lib.srl__streaming_edge_service__free
    to_service_func = lib.srl__streaming_edge_service__to_service

    get_cdn_region_func = lib.srl__streaming_edge_service__get_cdn_region
    get_pop_func = lib.srl__streaming_edge_service__get_pop

    get_capacity_func = lib.srl__streaming_edge_service__get_capacity
    get_load_func = lib.srl__streaming_edge_service__get_load
    get_relative_load_func = lib.srl__streaming_edge_service__get_relative_load


class StreamingMasterService(CdnServiceMixin, ServiceCapacityMixin, Service):
    """
    Streaming master service.
    """

    free_func = lib.srl__streaming_master_service__free
    to_service_func = lib.srl__streaming_master_service__to_service

    get_cdn_region_func = lib.srl__streaming_master_service__get_cdn_region
    get_pop_func = lib.srl__streaming_master_service__get_pop

    get_capacity_func = lib.srl__streaming_master_service__get_capacity


class ArrowAsnsService(CdnServiceMixin, ServiceCapacityMixin, Service):
    """
    Arrow ASNS service.
    """

    free_func = lib.srl__arrow_asns_service__free
    to_service_func = lib.srl__arrow_asns_service__to_service

    get_cdn_region_func = lib.srl__arrow_asns_service__get_cdn_region
    get_pop_func = lib.srl__arrow_asns_service__get_pop

    get_capacity_func = lib.srl__arrow_asns_service__get_capacity


class Consul(NativeObject):
    """
    Local image of a remote Consul based service DB.
    """

    def __init__(self, raw_ptr, loop=None):
        super().__init__(raw_ptr, free_func=None)

        self.event_loop = loop or asyncio.get_event_loop()

        self.update_callbacks = {}
        self.change_callbacks = {}

    async def sync(self):
        """
        Synchronize with the remote Consul API.
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
                exc = Exception(err)
                set_exception_threadsafe(exc)

        ncb = lib.SYNCHRONIZE_CALLBACK(callback)

        assert self.raw_ptr is not None

        lib.srl__consul__synchronize_async(self.raw_ptr, ncb)

        await fut

    def get_services(self, iterator_func, itertor_next_func, iterator_free_func, service_factory):
        assert self.raw_ptr is not None

        services = []

        iterator = iterator_func(self.raw_ptr)

        try:
            while True:
                service = itertor_next_func(iterator)
                if service is None:
                    break

                services.append(service_factory(service))
        finally:
            iterator_free_func(iterator)

        return services

    def get_streaming_master_services(self):
        """
        Get a list of all streaming master services.
        """
        return self.get_services(
            lib.srl__consul__get_all_streaming_master_services,
            lib.srl__streaming_master_services__next,
            lib.srl__streaming_master_services__free,
            StreamingMasterService)

    def get_streaming_edge_services(self):
        """
        Get a list of all streaming edge services.
        """
        return self.get_services(
            lib.srl__consul__get_all_streaming_edge_services,
            lib.srl__streaming_edge_services__next,
            lib.srl__streaming_edge_services__free,
            StreamingEdgeService)

    def get_arrow_asns_services(self):
        """
        Get a list of all Arrow ASNS services.
        """
        return self.get_services(
            lib.srl__consul__get_all_arrow_asns_services,
            lib.srl__arrow_asns_services__next,
            lib.srl__arrow_asns_services__free,
            ArrowAsnsService)

    def add_update_callback(self, cb):
        """
        Add a given update callback. The callback will be invoked whenever
        the consul service gets updated.
        """
        def callback(err):
            cb()

        ncb = lib.UPDATE_CALLBACK(callback)

        assert self.raw_ptr is not None

        token = lib.srl__consul__add_update_callback(self.raw_ptr, ncb)

        self.update_callbacks[token] = ncb

        return token

    def remove_update_callback(self, token):
        """
        Remove an update callback corresponding to a given token.
        """
        assert self.raw_ptr is not None

        lib.srl__consul__remove_update_callback(self.raw_ptr, token)

        self.update_callbacks.pop(token, None)

    def add_routing_changed_callback(self, cb):
        """
        Add a given routing-changed callback. The callback will be invoked
        whenever the routing information changes.
        """
        ncb = lib.CHANGE_CALLBACK(cb)

        assert self.raw_ptr is not None

        token = lib.srl__consul__add_change_callback(self.raw_ptr, ncb)

        self.change_callbacks[token] = ncb

        return token

    def remove_routing_changed_callback(self, token):
        """
        Remove a routing-changed callback corresponding to a given token.
        """
        assert self.raw_ptr is not None

        lib.srl__consul__remove_change_callback(self.raw_ptr, token)

        self.change_callbacks.pop(token, None)

    def is_healthy(self):
        """
        Check if the consul service is healthy.
        """
        assert self.raw_ptr is not None

        return lib.srl__consul__get_status(self.raw_ptr) == lib.STATUS_OK

    def exception(self):
        """
        Get the last synchronization exception.
        """
        assert self.raw_ptr is not None

        err = get_string(lib.srl__consul__get_error, self.raw_ptr)
        if err is None:
            return None

        return SynchronizationError(err)

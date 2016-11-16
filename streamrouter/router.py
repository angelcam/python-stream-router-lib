import hmac_tokens
import mmh3
import re
import time

from streamrouter.consul import ConsulClient


class Resource(object):

    COMMON = 'common'
    ARROW = 'arrow'
    AOC = 'aoc'

    re_camera_id = re.compile(r"^\D*(\d+)$")
    re_arrow_uuid = re.compile(r"^([0-9a-fA-F]{8})[0-9a-fA-F-]*$")

    def __init__(self, camera_id, setup=COMMON, arrow_uuid=None):
        camera_id = str(camera_id)

        m_camera_id = self.re_camera_id.match(camera_id)

        assert m_camera_id
        assert setup in [self.COMMON, self.ARROW, self.AOC]
        assert setup != self.ARROW or self.re_arrow_uuid.match(arrow_uuid)

        self.camera_id = camera_id
        self.numeric_camera_id = int(m_camera_id.group(1))

        self.setup = setup

        self.arrow_uuid = arrow_uuid

    def is_arrow_setup(self):
        return self.setup == self.ARROW


class Route(object):

    def __init__(self, url, token):
        self.url = url
        self.token = token


class RtspconRoute(Route):

    def __init__(self, url, token, master):
        super().__init__(url, token)

        self.master = master


class HlsEdgeRoute(Route):

    def __init__(self, url, token, master, edge):
        super().__init__(url, token)

        self.master = master
        self.edge = edge


class MjpegProxyRoute(Route):

    def __init__(self, url, token, proxy):
        super().__init__(url, token)

        self.proxy = proxy


class StreamRouter(object):

    def __init__(self, config, loop=None):
        self.__config = config
        self.__consul = ConsulClient(config, loop=loop)

    async def sync(self):
        await self.__consul.sync()

    def assign_rtspcon_service(self, region, resource):
        assert type(region) is str
        assert type(resource) is Resource

        n = resource.numeric_camera_id

        if resource.is_arrow_setup():
            asns = self.assign_arrow_asns_service(region, resource.arrow_uuid)
            if not asns:
                return None

            services = self.__consul.get_rtspcon_services_by_node_id(asns.node)

            return self.__capacity_aware_routing_algorithm(services, n)

        services = self.__consul.get_rtspcon_services(region)
        service = self.__capacity_aware_routing_algorithm(services, n)

        if not service:
            services = self.__consul.get_rtspcon_services()
            service = self.__capacity_aware_routing_algorithm(services, n)

        return service

    def assign_hls_edge_service(self, region, pop=None):
        assert type(region) is str
        assert pop is None or type(pop) is str

        services = []

        if pop:
            services = self.__consul.get_hls_edge_services(pop)

        # if the requested POP is too loaded, use also other servers in the
        # region (in order to avoid edge server congestion)
        if services:
            svc = services[0]
            rload = svc.load / svc.capacity
            if rload > 0.5:
                services = []

        if not services:
            services = self.__consul.get_hls_edge_services(region)

        if not services:
            services = self.__consul.get_hls_edge_services()

        if services:
            return services[0]

        return None

    def assign_mjpeg_proxy_service(self, region, resource):
        assert type(region) is str
        assert type(resource) is Resource

        if resource.is_arrow_setup():
            # TODO: for arrow setups, we should try to assign an MJPEG proxy
            # in the same data center as the corresponding ASNS server at first
            pass

        n = resource.numeric_camera_id

        services = self.__consul.get_mjpeg_proxy_services(region)
        service = self.__capacity_aware_routing_algorithm(services, n)

        if not service:
            services = self.__consul.get_mjpeg_proxy_services()
            service = self.__capacity_aware_routing_algorithm(services, n)

        return service

    def assign_arrow_asns_service(self, region, arrow_uuid):
        m_arrow_uuid = Resource.re_arrow_uuid.match(arrow_uuid)

        assert type(region) is str
        assert m_arrow_uuid

        n = int(m_arrow_uuid.group(1), 16)

        services = self.__consul.get_arrow_asns_services(region)

        return self.__capacity_aware_routing_algorithm(services, n)

    def construct_rtspcon_url(self, region, resource, ttl=None):
        route = self.construct_rtspcon_route(region, resource, ttl=ttl)
        if route:
            return route.url

        return None

    def construct_hls_edge_url(self, region, resource, ttl=None):
        route = self.construct_hls_edge_route(region, resource, ttl=ttl)
        if route:
            return route.url

        return None

    def construct_mjpeg_proxy_url(self, region, resource, ttl=None):
        route = self.construct_mjpeg_proxy_route(region, resource, ttl=ttl)
        if route:
            return route.url

        return None

    def construct_rtspcon_route(self, region, resource, ttl=None):
        master = self.assign_rtspcon_service(region, resource)
        if not master:
            return None

        token = self.get_rtspcon_token(resource.camera_id, ttl=ttl)
        url = master.url_mask % (resource.camera_id, token)

        return RtspconRoute(url, token, master)

    def construct_hls_edge_route(self, region, resource, ttl=None):
        master = self.assign_rtspcon_service(region, resource)
        if not master:
            return None

        edge = self.assign_hls_edge_service(region, master.pop)
        if not edge:
            return None

        token = self.get_rtspcon_token(resource.camera_id, ttl=ttl)
        mkey = master.id.replace('rtsp-master-', '')
        url = edge.url_mask % (mkey, resource.camera_id, token)

        return HlsEdgeRoute(url, token, master, edge)

    def construct_mjpeg_proxy_route(self, region, resource, ttl=None):
        proxy = self.assign_mjpeg_proxy_service(region, resource)
        if not proxy:
            return None

        token = self.get_mjpeg_proxy_token(resource.camera_id, ttl=ttl)
        url = proxy.url_mask % (resource.camera_id, token)

        return MjpegProxyRoute(url, token, proxy)

    def get_rtspcon_token(self, camera_id, ttl=None):
        if ttl is None:
            ttl = self.__config.rtspcon_ttl

        assert type(ttl) is int

        camera_id = str(camera_id)
        timestamp = time.time()

        return hmac_tokens.gen_token(
            self.__config.rtspcon_secret,
            token_time=int(timestamp * 1000000),
            timeout=ttl,
            alias=camera_id)

    def get_mjpeg_proxy_token(self, camera_id, ttl=None):
        if ttl is None:
            ttl = self.__config.mjpeg_proxy_ttl

        assert type(ttl) is int

        camera_id = str(camera_id)

        return hmac_tokens.gen_token(
            self.__config.mjpeg_proxy_secret,
            timeout=ttl,
            camera_id=camera_id)

    def add_update_callback(self, cb):
        self.__consul.add_update_callback(cb)

    def remove_update_callback(self, cb):
        self.__consul.remove_update_callback(cb)

    def add_routing_changed_callback(self, cb):
        self.__consul.add_routing_changed_callback(cb)

    def remove_routing_changed_callback(self, cb):
        self.__consul.remove_routing_changed_callback(cb)

    def is_healthy(self):
        return self.__consul.is_healthy()

    def exception(self):
        return self.__consul.exception()

    def close(self):
        self.__consul.close()

    def __capacity_aware_routing_algorithm(self, services, n):
        # get total service capacity
        tc = sum(map(lambda svc: svc.capacity, services))

        n = self.__get_hash(n) & 0x00ffffff

        while services:
            sf = (1 << 24) / tc     # capacity scaling factor
            cc = 0                  # cumulative capacity
            i = 0

            # get interval index
            while (i + 1) < len(services):
                svc = services[i]
                cc += svc.capacity
                if n < (cc * sf):
                    break

                i += 1

            svc = services.pop(i)

            if svc.healthy:
                return svc

            n = self.__get_hash(n) & 0x00ffffff

            tc -= svc.capacity

        return None

    def __get_hash(self, n):
        return mmh3.hash('%08x' % n) & 0xffffffff

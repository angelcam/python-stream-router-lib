import hmac_tokens
import mmh3
import re
import time
import logging

from streamrouter import ConsulClient

logger = logging.getLogger(__name__)


class NotImplementedStream(Exception):
    def __init__(self, request_format, possible_formats):
        self.request_stream_format = request_format
        self.possible_formats = possible_formats


class ServiceUnavailable(Exception):
    def __init__(self, service_name):
        self.service_name = service_name


class Resource(object):
    COMMON = 'common'
    ARROW = 'arrow'
    AOC = 'aoc'

    # camera in preview stream have id with non-numeric prefix
    re_camera_id = re.compile(r"^\D*(\d+)$")
    re_arrow_uuid = re.compile(r"^([0-9a-fA-F]{8})[0-9a-fA-F-]*$")

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

    def is_arrow_setup(self):
        return self.setup == self.ARROW


class Route(object):
    STREAM_FORMAT_HLS = "hls"
    STREAM_FORMAT_MP4 = "mp4"
    STREAM_FORMAT_MJPEG = "mjpeg"
    LIVE_SNAPSHOT = "snapshot"

    url_mask = None
    possible_streams = {}

    def __init__(self, token, resource):
        self.token = token
        self.resource = resource

    def get_url(self, stream_format):
        if stream_format in self.possible_streams:
            kwargs = self._get_kwargs(stream_format=stream_format)
            return self.url_mask.format(**kwargs)
        else:
            raise NotImplementedStream(stream_format, self.possible_streams)

    def _host(self, stream_format):
        service = self._service(stream_format)
        if service['service'] is not None:
            return service['service'].host
        else:
            raise ServiceUnavailable(service['name'])

    def _service(self, stream_format):
        raise NotImplementedError

    def _get_kwargs(self, stream_format):
        return {
            'host': self._host(stream_format),
            'camera_id': self.resource.camera_id,
            'token': self.token,
            'stream_name': self.possible_streams[stream_format]
        }

    @property
    def mjpeg_url(self):
        return self.get_url(self.STREAM_FORMAT_MJPEG)

    @property
    def hls_url(self):
        return self.get_url(self.STREAM_FORMAT_HLS)

    @property
    def mp4_url(self):
        return self.get_url(self.STREAM_FORMAT_MP4)

    @property
    def snapshot_url(self):
        return self.get_url(self.LIVE_SNAPSHOT)


class RtspconRoute(Route):
    url_mask = 'https://{host}/stream/{camera_id}/{stream_name}?token={token}'

    possible_streams = {
        Route.STREAM_FORMAT_HLS: 'playlist.m3u8',
        Route.STREAM_FORMAT_MP4: 'stream.mp4',
        Route.STREAM_FORMAT_MJPEG: 'stream.mjpeg',
        Route.LIVE_SNAPSHOT: 'snapshot.jpg'
    }

    def __init__(self, resource, token, master):
        super().__init__(token, resource)
        self.master = master

    def _service(self, stream_format):
        return {'name': 'rtsp-master', 'service': self.master}


class EdgeRoute(RtspconRoute):
    url_mask = 'https://{host}/{master}/{camera_id}/{stream_name}?token={token}'

    possible_streams = {
        Route.STREAM_FORMAT_HLS: 'playlist.m3u8',
        Route.STREAM_FORMAT_MP4: 'stream.mp4',
    }

    def __init__(self, resource, token, master, rtsp_edge, mp4_edge):
        super().__init__(resource, token, master)
        self.rtsp_edge = rtsp_edge
        self.mp4_edge = mp4_edge

    def _service(self, stream_format):
        if self.master is None:
            raise ServiceUnavailable('rtsp-master')
        if stream_format == self.STREAM_FORMAT_MP4:
            return {'name': 'mp4-edge', 'service': self.mp4_edge}
        return {'name': 'rtsp-edge', 'service': self.rtsp_edge}

    @property
    def master_key(self):
        return self.master.id.replace('rtsp-master-', '')

    def _get_kwargs(self, stream_format, **kwargs):
        kwargs = super()._get_kwargs(stream_format=stream_format)
        kwargs['master'] = self.master_key
        return kwargs


class MjpegProxyRoute(Route):
    url_mask = 'https://{host}/{stream_name}/{camera_id}?token={token}'

    possible_streams = {
        Route.STREAM_FORMAT_MJPEG: 'stream',
        Route.LIVE_SNAPSHOT: 'snapshot'
    }

    def __init__(self, resource, token, proxy):
        super().__init__(token, resource)
        self.proxy = proxy

    def _service(self, stream_format):
        return {'name': 'mjpeg-proxy', 'service': self.proxy}


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
            service = self.__capacity_aware_routing_algorithm(services, n)
            if not service:
                logger.error("No RTSPCon services available with requested node id.", extra={'node_id': asns.node})
            return service

        services = self.__consul.get_rtspcon_services(region)
        service = self.__capacity_aware_routing_algorithm(services, n)

        if not service:
            logger.warning("No RTSPCon services available in the region.", extra={'region': region})
            services = self.__consul.get_rtspcon_services()
            service = self.__capacity_aware_routing_algorithm(services, n)
        if not service:
            logger.error("No RTSPCon services available at all.")
        return service

    def assign_rtsp_edge_service(self, region, pop=None):
        assert type(region) is str
        assert pop is None or type(pop) is str

        services = []

        if pop:
            services = self.__consul.get_rtsp_edge_services(pop)

        # if the requested POP is too loaded, use also other servers in the
        # region (in order to avoid edge server congestion)
        if services:
            svc = services[0]
            rload = svc.load / svc.capacity
            if rload > 0.5:
                services = []

        if not services:
            logger.warning("No RTSP Edge services available with requested POP.", extra={'region': region, 'pop': pop})
            services = self.__consul.get_rtsp_edge_services(region)

        if not services:
            logger.warning("No RTSP Edge services available in the region.", extra={'region': region})
            services = self.__consul.get_rtsp_edge_services()

        if services:
            return services[0]

        logger.error("No RTSP Edge services available at all.")
        return None

    def assign_mp4_edge_service(self, region, pop=None):
        assert type(region) is str
        assert pop is None or type(pop) is str

        services = []

        if pop:
            services = self.__consul.get_mp4_edge_services(pop)

        # if the requested POP is too loaded, use also other servers in the
        # region (in order to avoid edge server congestion)
        if services:
            svc = services[0]
            rload = svc.load / svc.capacity
            if rload > 0.5:
                services = []

        if not services:
            logger.warning("No MP4 Edge services available with requested POP.", extra={'region': region, 'pop': pop})
            services = self.__consul.get_mp4_edge_services(region)

        if not services:
            logger.warning("No MP4 Edge services available in the region.", extra={'region': region})
            services = self.__consul.get_mp4_edge_services()

        if services:
            return services[0]

        logger.error("No MP4 Edge services available at all.")
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
            logger.warning("No MJPEG proxy services available in the region.", extra={'region': region})
            services = self.__consul.get_mjpeg_proxy_services()
            service = self.__capacity_aware_routing_algorithm(services, n)

        if not service:
            logger.error("No MJPEG proxy services available at all.")
        return service

    def assign_arrow_asns_service(self, region, arrow_uuid):
        m_arrow_uuid = Resource.re_arrow_uuid.match(arrow_uuid)

        assert type(region) is str
        assert m_arrow_uuid

        n = int(m_arrow_uuid.group(1), 16)

        services = self.__consul.get_arrow_asns_services(region)
        service = self.__capacity_aware_routing_algorithm(services, n)

        if not service:
            logger.error("No ASNS services available in the region.", extra={'region': region})
        return service

    def construct_rtspcon_route(self, region, resource, ttl=None):
        master = self.assign_rtspcon_service(region, resource)

        token = self.get_rtspcon_token(resource.camera_id, ttl=ttl)
        return RtspconRoute(resource, token, master)

    def construct_edge_route(self, region, resource, ttl=None):
        master = self.assign_rtspcon_service(region, resource)
        pop = None
        if master is not None:
            pop = master.pop
        rtsp_edge = self.assign_rtsp_edge_service(region, pop)
        mp4_edge = self.assign_mp4_edge_service(region, pop)
        token = self.get_rtspcon_token(resource.camera_id, ttl=ttl)
        return EdgeRoute(resource, token, master, rtsp_edge, mp4_edge)

    def construct_mjpeg_proxy_route(self, region, resource, ttl=None):
        proxy = self.assign_mjpeg_proxy_service(region, resource)
        token = self.get_mjpeg_proxy_token(resource.camera_id, ttl=ttl)
        return MjpegProxyRoute(resource, token, proxy)

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
            sf = (1 << 24) / tc  # capacity scaling factor
            cc = 0  # cumulative capacity
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

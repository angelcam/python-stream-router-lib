import asyncio
import consul.aio
import json
import logging
import re
import sys

logger = logging.getLogger(__name__)


def int_or_none(value):
    try:
        return int(value)
    except Exception:
        return None


def str_or_none(value):
    if value is None:
        return value

    return str(value)


class SynchronizationError(Exception):
    def __init__(self, inner):
        super().__init__()
        self.inner = inner

    def __str__(self):
        if isinstance(self.inner, list):
            cause = ', '.join(map(lambda elem: str(elem), self.inner))
        else:
            cause = str(self.inner)

        return "Consul synchronization error, cause: %s" % cause


class Service(object):
    def __init__(self, consul_svc, kv_items):
        self.node = consul_svc['Node']['Node']

        self.id = consul_svc['Service']['ID']

        self.host = consul_svc['Service']['Address']
        self.tags = consul_svc['Service']['Tags']

        self.pop = self.__get_pop(self.tags)

        self.healthy = all(map(
            lambda check: check['Status'].lower() == 'passing',
            consul_svc['Checks']))

        self.kv = kv_items.get(self.id, {})

        self.load = int_or_none(self.kv.get('bandwidth'))
        self.capacity = int_or_none(self.kv.get('maxBandwidth'))

        if 'disabled' in self.kv and self.kv['disabled'] != '0':
            self.healthy = False

        self.services = {}

    def __eq__(self, other):
        return type(self) == type(other) \
               and self.id == other.id \
               and self.node == other.node \
               and self.pop == other.pop \
               and self.healthy == other.healthy \
               and self.capacity == other.capacity

    def __ne__(self, other):
        return not self.__eq__(other)

    def __get_pop(self, tags):
        for tag in tags:
            if tag.startswith('pop_'):
                return tag

        return None


class ServiceMap(object):
    def __init__(self, services):
        self.__tag_map = {}
        self.__node_map = {}

        self.__all = services

        for svc in services:
            # add this service to all corresponding tags
            for tag in svc.tags:
                if tag not in self.__tag_map:
                    self.__tag_map[tag] = []
                self.__tag_map[tag].append(svc)

            node = svc.node

            # add this service to the corresponding node
            if node not in self.__node_map:
                self.__node_map[node] = []
            self.__node_map[node].append(svc)

    def __eq__(self, other):
        return type(self) == type(other) \
               and self.__all == other.__all

    def __ne__(self, other):
        return not self.__eq__(other)

    def __by_key(self, svc_map, key):
        if key in svc_map:
            return svc_map[key]

        return []

    def by_tag(self, tag):
        return self.__by_key(self.__tag_map, tag)

    def by_node_id(self, node_id):
        return self.__by_key(self.__node_map, node_id)

    def all(self):
        return self.__all


class ConsulClient(object):
    def __init__(self, config, loop=None):
        self.__config = config

        self.__re_kv_key = re.compile(r"^service/([^\s/]+)/([^\s/]+)/(\S+)$")

        self.__update_callbacks = set()
        self.__routing_changed_callbacks = set()

        self.__last_error = None
        self.__closed = False

        if loop is None:
            loop = asyncio.get_event_loop()

        self.__event_loop = loop

        self.__rtspcon_services = ServiceMap([])
        self.__rtsp_edge_services = ServiceMap([])
        self.__mp4_edge_services = ServiceMap([])
        self.__mjpeg_proxy_services = ServiceMap([])
        self.__arrow_asns_services = ServiceMap([])

        self.__sync_task = loop.create_task(self.__sync_loop())

    async def sync(self):
        await self.__sync()

    async def __sync_loop(self):
        try:
            while not self.__closed:
                await self.__sync()
                await asyncio.sleep(
                    self.__config.sync_period,
                    loop=self.__event_loop)
        except asyncio.CancelledError:
            pass

    async def __sync(self):
        old_rtspcon_services = self.__rtspcon_services
        old_rtsp_edge_services = self.__rtsp_edge_services
        old_mp4_edge_services = self.__mp4_edge_services
        old_mjpeg_proxy_services = self.__mjpeg_proxy_services
        old_arrow_asns_services = self.__arrow_asns_services

        tasks = [
            self.__load_rtspcon_services(),
            self.__load_rtsp_edge_services(),
            self.__load_mp4_edge_services(),
            self.__load_mjpeg_proxy_services(),
            self.__load_arrow_asns_services(),
        ]

        errors = []

        for task in tasks:
            try:
                await asyncio.wait_for(task, 10.0, loop=self.__event_loop)
            except asyncio.CancelledError:
                raise
            except KeyboardInterrupt:
                raise
            except Exception as ex:
                logger.warning(
                    "Consul synchronization error",
                    exc_info=sys.exc_info())
                errors.append(ex)

        if errors:
            self.__last_error = SynchronizationError(errors)
        else:
            self.__last_error = None

        self.__on_update()

        if old_rtspcon_services != self.__rtspcon_services \
                or old_rtsp_edge_services != self.__rtsp_edge_services \
                or old_mp4_edge_services != self.__mp4_edge_services \
                or old_mjpeg_proxy_services != self.__mjpeg_proxy_services \
                or old_arrow_asns_services != self.__arrow_asns_services:
            self.__on_routing_change()

    async def __load_services(self, svc_name):
        c = consul.aio.Consul(
            host=self.__config.consul_host,
            port=self.__config.consul_port,
            loop=self.__event_loop)

        key = 'service/%s' % svc_name

        index, services = await c.health.service(svc_name)
        index, kv_items = await c.kv.get(key, recurse=True)

        if services is None:
            services = []

        if kv_items is None:
            kv_items = []

        kv_items = self.__parse_kv_items(kv_items)

        return list(map(
            lambda svc: Service(svc, kv_items),
            services))

    async def __load_rtspcon_services(self):
        services = await self.__load_services('rtsp-master')

        services = filter(lambda svc: svc.pop is not None, services)
        services = filter(lambda svc: svc.capacity is not None, services)
        services = filter(lambda svc: svc.capacity > 0, services)

        services = sorted(services, key=lambda svc: svc.id)

        self.__rtspcon_services = ServiceMap(services)

    async def __load_rtsp_edge_services(self):
        services = await self.__load_services('rtsp-edge')

        services = filter(lambda svc: svc.healthy, services)
        services = filter(lambda svc: svc.pop is not None, services)
        services = filter(lambda svc: svc.load is not None, services)
        services = filter(lambda svc: svc.capacity is not None, services)
        services = filter(lambda svc: svc.capacity > 0, services)
        services = filter(lambda svc: svc.load < svc.capacity, services)

        services = sorted(services, key=lambda svc: svc.load / svc.capacity)

        self.__rtsp_edge_services = ServiceMap(services)

    async def __load_mp4_edge_services(self):
        services = await self.__load_services('mp4-edge')

        services = filter(lambda svc: svc.healthy, services)
        services = filter(lambda svc: svc.pop is not None, services)
        services = filter(lambda svc: svc.load is not None, services)
        services = filter(lambda svc: svc.capacity is not None, services)
        services = filter(lambda svc: svc.capacity > 0, services)
        services = filter(lambda svc: svc.load < svc.capacity, services)

        services = sorted(services, key=lambda svc: svc.load / svc.capacity)

        self.__mp4_edge_services = ServiceMap(services)

    async def __load_mjpeg_proxy_services(self):
        services = await self.__load_services('mjpeg-proxy')

        services = filter(lambda svc: svc.pop is not None, services)
        services = filter(lambda svc: svc.capacity is not None, services)
        services = filter(lambda svc: svc.capacity > 0, services)

        services = sorted(services, key=lambda svc: svc.id)

        self.__mjpeg_proxy_services = ServiceMap(services)

    async def __load_arrow_asns_services(self):
        services = await self.__load_services('arrow-asns')

        asns_port = self.__config.arrow_asns_port
        asns_api_port = self.__config.arrow_asns_api_port
        rtsp_proxy_port = self.__config.arrow_asns_rtsp_proxy_port
        http_proxy_port = self.__config.arrow_asns_http_proxy_port

        for svc in services:
            rtspcons = self.__rtspcon_services.by_node_id(svc.node)
            nhealthy = any(map(lambda rcon: rcon.healthy, rtspcons))

            svc.healthy = svc.healthy and nhealthy

            svc.services = {
                "asns": "%s:%d" % (svc.host, asns_port),
                "asns_api": "%s:%d" % (svc.host, asns_api_port),
                "rtsp_proxy": "%s:%d" % (svc.host, rtsp_proxy_port),
                "http_proxy": "%s:%d" % (svc.host, http_proxy_port),
            }

        services = filter(lambda svc: svc.pop is not None, services)
        services = filter(lambda svc: svc.capacity is not None, services)
        services = filter(lambda svc: svc.capacity > 0, services)

        services = sorted(services, key=lambda svc: svc.id)

        self.__arrow_asns_services = ServiceMap(services)

    def __parse_kv_items(self, items):
        res = {}

        for item in items:
            m_key = self.__re_kv_key.match(item['Key'])

            # ignore keys that do not match the pattern
            if not m_key:
                continue

            svc = m_key.group(2)
            key = m_key.group(3)

            if svc not in res:
                res[svc] = {}

            try:
                res[svc][key] = self.__parse_kv_item(item)
            except Exception:
                # ignore invalid keys
                pass

        return res

    def __parse_kv_item(self, item):
        flags = item['Flags']
        value = item['Value']

        value = value.decode('utf-8')

        if flags == 0x01:
            return int(value)
        elif flags == 0x02:
            return float(value)
        elif flags == 0x03:
            return json.loads(value)

        return value

    def __get_services(self, services, tag):
        if tag is None:
            return list(services.all())

        return list(services.by_tag(tag))

    def __on_update(self):
        for callback in self.__update_callbacks:
            try:
                callback()
            except Exception:
                # ignore errors in user callbacks
                pass

    def __on_routing_change(self):
        for callback in self.__routing_changed_callbacks:
            try:
                callback()
            except Exception:
                # ignore errors in user callbacks
                pass

    def get_rtspcon_services(self, tag=None):
        return self.__get_services(self.__rtspcon_services, tag)

    def get_rtspcon_services_by_node_id(self, node_id):
        return list(self.__rtspcon_services.by_node_id(node_id))

    def get_rtsp_edge_services(self, tag=None):
        return self.__get_services(self.__rtsp_edge_services, tag)

    def get_mp4_edge_services(self, tag=None):
        return self.__get_services(self.__mp4_edge_services, tag)

    def get_mjpeg_proxy_services(self, tag=None):
        return self.__get_services(self.__mjpeg_proxy_services, tag)

    def get_arrow_asns_services(self, tag=None):
        return self.__get_services(self.__arrow_asns_services, tag)

    def add_update_callback(self, cb):
        self.__update_callbacks.add(cb)

    def remove_update_callback(self, cb):
        self.__update_callbacks.remove(cb)

    def add_routing_changed_callback(self, cb):
        self.__routing_changed_callbacks.add(cb)

    def remove_routing_changed_callback(self, cb):
        self.__routing_changed_callbacks.remove(cb)

    def is_healthy(self):
        return self.__last_error is None

    def exception(self):
        return self.__last_error

    def close(self):
        if self.__closed:
            return

        self.__closed = True

        self.__sync_task.cancel()

        try:
            self.__event_loop.run_until_complete(
                self.__sync_task)
        except asyncio.CancelledError:
            pass

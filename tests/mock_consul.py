import asyncio
import copy
import json
import os

from aiohttp.web import Application, Response


def load_json(fname):
    with open(fname, "rt") as f:
        return json.load(f)


class MockConsul(object):
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.__event_loop = loop

        tdir = os.path.dirname(__file__)
        hdir = os.path.join(tdir, 'health')
        kdir = os.path.join(tdir, 'kv')

        self.__health = {}
        self.__kv = {}

        self.__health['rtsp-master'] = load_json(os.path.join(hdir, "rtsp-master.json"))
        self.__health['rtsp-edge'] = load_json(os.path.join(hdir, "rtsp-edge.json"))
        self.__health['mp4-edge'] = load_json(os.path.join(hdir, "mp4-edge.json"))
        self.__health['mjpeg-proxy'] = load_json(os.path.join(hdir, "mjpeg-proxy.json"))
        self.__health['arrow-asns'] = load_json(os.path.join(hdir, "arrow-asns.json"))

        self.__kv['rtsp-master'] = load_json(os.path.join(kdir, "rtsp-master.json"))
        self.__kv['rtsp-edge'] = load_json(os.path.join(kdir, "rtsp-edge.json"))
        self.__kv['mp4-edge'] = load_json(os.path.join(kdir, "mp4-edge.json"))
        self.__kv['mjpeg-proxy'] = load_json(os.path.join(kdir, "mjpeg-proxy.json"))
        self.__kv['arrow-asns'] = load_json(os.path.join(kdir, "arrow-asns.json"))

        self.health = copy.deepcopy(self.__health)
        self.kv = copy.deepcopy(self.__kv)

        app = Application(loop=loop)

        app.router.add_route('GET', r"/v1/health/service/{svc}", self.__handle_health_request)
        app.router.add_route('GET', r"/v1/kv/{svc:.+}", self.__handle_kv_request)

        self.__app = app
        self.__handler = None
        self.__server = None

    def __handle_health_request(self, request):
        headers = {
            'X-Consul-Index': '1'
        }

        svc = request.match_info['svc']
        if svc not in self.health:
            return Response(status=404, headers=headers)

        data = self.health[svc]

        return Response(
            content_type="application/json",
            headers=headers,
            text=json.dumps(data))

    def __handle_kv_request(self, request):
        headers = {
            'X-Consul-Index': '1'
        }

        svc = request.match_info['svc']
        svc = svc.split('/')

        if len(svc) < 2 or svc[1] not in self.kv:
            return Response(status=404, headers=headers)

        data = self.kv[svc[1]]

        return Response(
            content_type="application/json",
            headers=headers,
            text=json.dumps(data))

    def listen(self, address, port):
        if self.__server is not None:
            return

        self.__handler = self.__app.make_handler()

        coro = self.__event_loop.create_server(
            self.__handler,
            address,
            port)

        self.__server = self.__event_loop.run_until_complete(coro)

    def reset(self):
        self.health = copy.deepcopy(self.__health)
        self.kv = copy.deepcopy(self.__kv)

    def close(self):
        if self.__server is None:
            return

        self.__server.close()
        self.__event_loop.run_until_complete(
            self.__close())

        self.__handler = None
        self.__server = None

    async def __close(self):
        await self.__server.wait_closed()
        await self.__app.shutdown()
        await self.__handler.finish_connections(10.0)
        await self.__app.cleanup()

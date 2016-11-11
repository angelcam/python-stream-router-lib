import asyncio
import pytest
import re

from tests.mock_consul import MockConsul
from streamrouter.router import StreamRouter, Resource
from streamrouter.config import RouterConfig


@pytest.fixture
def event_loop():
    return asyncio.get_event_loop()


@pytest.yield_fixture
def consul(event_loop):
    consul = MockConsul(loop=event_loop)
    consul.listen('0.0.0.0', 6666)
    yield consul
    consul.close()


@pytest.fixture
def config():
    config = RouterConfig()
    config.consul_host = '127.0.0.1'
    config.consul_port = 6666
    config.sync_period = 0
    config.rtspcon_secret = 'hello'
    config.mjpeg_proxy_secret = 'world'
    return config


@pytest.yield_fixture
def router(config, consul, event_loop):
    router = StreamRouter(config, loop=event_loop)
    router.add_update_callback(event_loop.stop)
    event_loop.run_forever()
    yield router
    router.close()


def set_healthy(service, node, healthy, consul, router, event_loop):
    services = consul.health.get(service, [])
    for svc in services:
        if svc['Node']['Node'] != node:
            continue

        if healthy:
            svc['Checks'][0]['Status'] = 'passing'
        else:
            svc['Checks'][0]['Status'] = 'failed'

    event_loop.run_forever()

    assert router.is_healthy()


def set_healthy_by_svc_id(service, id, healthy, consul, router, event_loop):
    services = consul.health.get(service, [])
    for svc in services:
        if svc['Service']['ID'] != id:
            continue

        if healthy:
            svc['Checks'][0]['Status'] = 'passing'
        else:
            svc['Checks'][0]['Status'] = 'failed'

    event_loop.run_forever()

    assert router.is_healthy()


def remove_by_tag(service, tag, consul, router, event_loop):
    services = consul.health.get(service)
    if not services:
        return

    services = filter(lambda svc: tag not in svc['Service']['Tags'], services)

    consul.health[service] = list(services)

    event_loop.run_forever()

    assert router.is_healthy()


def remove_all(service, consul, router, event_loop):
    consul.health[service] = []
    event_loop.run_forever()
    assert router.is_healthy()


def test_initial_results(config, consul, event_loop):
    router = StreamRouter(config, loop=event_loop)

    cresource = Resource(0)
    aresource = Resource(0, setup=Resource.ARROW, arrow_uuid='012345678')

    assert router.construct_rtspcon_url('eu', cresource) is None
    assert router.construct_rtspcon_url('eu', aresource) is None
    assert router.construct_hls_edge_url('eu', cresource) is None
    assert router.construct_mjpeg_proxy_url('eu', cresource) is None

    router.close()


def test_update(router):
    assert router.is_healthy()


def test_rtspcon_url_common(consul, router, event_loop):
    region = 'eu'
    resource = Resource(17)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu3\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    set_healthy('rtsp-master', 'm1-eu1', False, consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu3\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    set_healthy('rtsp-master', 'm1-eu3', False, consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu2\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    remove_by_tag('rtsp-master', 'eu', consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-na1\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    remove_all('rtsp-master', consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert url is None


def test_rtspcon_url_arrow(consul, router, event_loop):
    region = 'eu'
    resource = Resource(
        16,
        setup=Resource.ARROW,
        arrow_uuid='00000011')

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu3\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    set_healthy('rtsp-master', 'm1-eu1', False, consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu3\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    set_healthy('rtsp-master', 'm1-eu3', False, consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu2\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    set_healthy('rtsp-master', 'm1-eu1', True, consul, router, event_loop)
    set_healthy('rtsp-master', 'm1-eu3', True, consul, router, event_loop)
    set_healthy('arrow-asns', 'm1-eu3', False, consul, router, event_loop)

    url = router.construct_rtspcon_url(region, resource)
    assert re.match(r"^http://m1-eu1\.angelcam\.com/stream/\d+/playlist\.m3u8\?token=.*$", url)  # noqa


def test_hls_edge_url(consul, router, event_loop):
    region = 'eu'
    resource = Resource(19)

    url = router.construct_hls_edge_url(region, resource)
    assert re.match(r"^http://e3-eu2\.angelcam\.com/m3-eu2/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    remove_by_tag('rtsp-edge', 'pop_eu2', consul, router, event_loop)

    url = router.construct_hls_edge_url(region, resource)
    assert re.match(r"^http://e1-eu3\.angelcam\.com/m3-eu2/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    remove_by_tag('rtsp-edge', 'eu', consul, router, event_loop)

    url = router.construct_hls_edge_url(region, resource)
    assert re.match(r"^http://e1-na5\.angelcam\.com/m3-eu2/\d+/playlist\.m3u8\?token=.*$", url)  # noqa

    remove_all('rtsp-edge', consul, router, event_loop)

    url = router.construct_hls_edge_url(region, resource)
    assert url is None


def test_mjpeg_proxy_url(consul, router, event_loop):
    region = 'eu'
    resource = Resource(17)

    url = router.construct_mjpeg_proxy_url(region, resource)
    assert re.match(r"^http://mjpeg1-eu2-3\.angelcam\.com/stream/\d+\?token=.*$", url)  # noqa

    set_healthy_by_svc_id(
        'mjpeg-proxy',
        'mjpeg-proxy-mjpeg1-eu2-3',
        False,
        consul,
        router,
        event_loop)

    url = router.construct_mjpeg_proxy_url(region, resource)
    assert re.match(r"^http://mjpeg1-eu2-1\.angelcam\.com/stream/\d+\?token=.*$", url)  # noqa

    remove_by_tag('mjpeg-proxy', 'eu', consul, router, event_loop)

    url = router.construct_mjpeg_proxy_url(region, resource)
    assert re.match(r"^http://mjpeg1-na1-3\.angelcam\.com/stream/\d+\?token=.*$", url)  # noqa

    remove_all('mjpeg-proxy', consul, router, event_loop)

    url = router.construct_mjpeg_proxy_url(region, resource)
    assert url is None


def test_unhealthy_arrow_asns_services(consul, router, event_loop):
    services = consul.health.get('arrow-asns', [])
    for svc in services:
        svc['Checks'][0]['Status'] = 'failed'
    event_loop.run_forever()

    assert router.is_healthy()

    region = 'eu'
    resource = Resource(
        16,
        setup=Resource.ARROW,
        arrow_uuid='00000011')

    route = router.construct_rtspcon_route(region, resource)

    assert route is None

# TODO: statistical tests + comparison tests

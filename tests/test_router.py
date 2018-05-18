import asyncio
import pytest
import re

from streamrouter import (
    ArrowAsnsService, EdgeRoute, HlsEdgeService, MjpegProxyRoute, MjpegProxyService,
    Mp4EdgeService, Resource, RouterConfig, RoutingFailed, RtspconRoute, RtspconService,
    StreamRouter, UnsupportedStreamFormat,
)

from tests.mock_consul import MockConsul


@pytest.fixture
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture
def config():
    config = RouterConfig()
    config.consul_host = '127.0.0.1'
    config.consul_port = 6666
    config.rtspcon_secret = 'hello'
    config.mjpeg_proxy_secret = 'world'
    return config


@pytest.yield_fixture
def consul(event_loop):
    consul = MockConsul(loop=event_loop)
    consul.listen('0.0.0.0', 6666)
    yield consul
    consul.close()


@pytest.yield_fixture
def router(config, consul, event_loop):
    router = StreamRouter(config, loop=event_loop)
    event_loop.run_until_complete(router.sync())
    yield router
    router.close()


def test_initial_results(config, consul, event_loop):
    router = StreamRouter(config, loop=event_loop)

    assert router.exception() is None
    assert not router.is_healthy()

    region = 'eu'
    resource = Resource(0)
    arrow_uuid = '00000000000000000000000000000000'

    assert router.assign_rtspcon_service(region, resource) is None
    assert router.assign_mjpeg_proxy_service(region, resource) is None
    assert router.assign_hls_edge_service(region) is None
    assert router.assign_mp4_edge_service(region) is None
    assert router.assign_arrow_asns_service(region, arrow_uuid) is None

    with pytest.raises(RoutingFailed):
        router.construct_rtspcon_route('eu', resource)

    with pytest.raises(RoutingFailed):
        router.construct_edge_route('eu', resource)

    with pytest.raises(RoutingFailed):
        router.construct_mjpeg_proxy_route('eu', resource)

    router.close()


def test_post_sync(router):
    assert router.exception() is None
    assert router.is_healthy()


def test_assign_service(router):
    region = 'eu'
    resource = Resource(0)
    arrow_uuid = '00000000000000000000000000000000'

    service = router.assign_rtspcon_service(region, resource)

    assert type(service) is RtspconService

    assert service.id == 'rtsp-master-m3-eu2'
    assert service.node_id == 'm3-eu2'
    assert service.host == 'm3-eu2.angelcam.com'
    assert service.region == 'eu'
    assert service.pop == 'eu2'
    assert service.capacity == 200
    assert service.healthy
    assert not service.disabled
    assert service.tags == set(['eu', 'pop_eu2'])
    assert service.params == {
        'activeStreams': '433',
        'disabled': '0',
        'maxBandwidth': '200',
        'urlMask': 'http://m3-eu2.angelcam.com/stream/%s/playlist.m3u8?token=%s',
    }

    service = router.assign_mjpeg_proxy_service(region, resource)

    assert type(service) is MjpegProxyService

    assert service.id == 'mjpeg-proxy-mjpeg1-eu2-4'
    assert service.node_id == 'mjpeg1-eu2'
    assert service.host == 'mjpeg1-eu2-4.angelcam.com'
    assert service.region == 'eu'
    assert service.pop == 'eu2'
    assert service.capacity == 200
    assert service.healthy
    assert not service.disabled
    assert service.tags == set(['eu', 'pop_eu2'])
    assert service.params == {
        'maxBandwidth': '200',
        'urlMask': 'http://mjpeg1-eu2-4.angelcam.com/stream/%s?token=%s',
    }

    service = router.assign_hls_edge_service(region)

    assert type(service) is HlsEdgeService

    assert service.id == 'e1-eu3.angelcam.com'
    assert service.node_id == 'e1-eu3'
    assert service.host == 'e1-eu3.angelcam.com'
    assert service.region == 'eu'
    assert service.pop == 'eu3'
    assert service.capacity == 512
    assert service.healthy
    assert not service.disabled
    assert service.tags == set(['eu', 'pop_eu3'])

    params = service.params

    try:
        del params['connectionsDetail']
        del params['modified']
    except KeyError:
        pass

    assert params == {
        'bandwidth': '229.25',
        'connections': '279',
        'continent': 'eu',
        'disabled': '0',
        'key': 'e1-eu3',
        'maxBandwidth': '512',
        'urlMask': 'http://e1-eu3.angelcam.com/%s/%s/playlist.m3u8?token=%s',
    }

    service = router.assign_mp4_edge_service(region)

    assert type(service) is Mp4EdgeService

    assert service.id == 'mp4-edge-e3-eu2.angelcam.com'
    assert service.node_id == 'e3-eu2'
    assert service.host == 'e3-eu2.angelcam.com'
    assert service.region == 'eu'
    assert service.pop == 'eu2'
    assert service.capacity == 512
    assert service.healthy
    assert not service.disabled
    assert service.tags == set(['eu', 'pop_eu2'])

    params = service.params

    try:
        del params['modified']
    except KeyError:
        pass

    assert params == {
        'maxBandwidth': '512',
        'bandwidth': '210.65',
        'continent': 'eu',
        'key': 'e3-eu2',
        'urlMask': 'https://e3-eu2.angelcam.com/%s/%s/stream.mp4?token=%s',
    }

    service = router.assign_arrow_asns_service(region, arrow_uuid)

    assert type(service) is ArrowAsnsService

    assert service.id == 'arrow-asns-m3-eu2'
    assert service.node_id == 'm3-eu2'
    assert service.host == 'm3-eu2.angelcam.com'
    assert service.region == 'eu'
    assert service.pop == 'eu2'
    assert service.capacity == 200
    assert service.healthy
    assert not service.disabled
    assert service.tags == set(['eu', 'pop_eu2'])
    assert service.params == {
        'maxBandwidth': '200',
    }


def test_construct_route(router):
    region = 'eu'
    resource = Resource(0)

    route = router.construct_edge_route(region, resource)

    assert type(route) is EdgeRoute

    assert re.match(r'https://e3-eu2\.angelcam\.com/m3-eu2/0/playlist\.m3u8\?token=\S*', route.hls_url)
    assert re.match(r'https://e3-eu2\.angelcam\.com/m3-eu2/0/stream\.mp4\?token=\S*', route.mp4_url)

    with pytest.raises(UnsupportedStreamFormat):
        route.mjpeg_url

    with pytest.raises(UnsupportedStreamFormat):
        route.snapshot_url

    route = router.construct_rtspcon_route(region, resource)

    assert type(route) is RtspconRoute

    assert re.match(r'https://m3-eu2\.angelcam\.com/stream/0/stream\.mjpeg\?token=\S*', route.mjpeg_url)
    assert re.match(r'https://m3-eu2\.angelcam\.com/stream/0/playlist\.m3u8\?token=\S*', route.hls_url)
    assert re.match(r'https://m3-eu2\.angelcam\.com/stream/0/stream\.mp4\?token=\S*', route.mp4_url)
    assert re.match(r'https://m3-eu2\.angelcam\.com/stream/0/snapshot\.jpg\?token=\S*', route.snapshot_url)

    route = router.construct_mjpeg_proxy_route(region, resource)

    assert type(route) is MjpegProxyRoute

    assert re.match(r'https://mjpeg1-eu2-4\.angelcam\.com/stream/0\?token=\S*', route.mjpeg_url)
    assert re.match(r'https://mjpeg1-eu2-4\.angelcam\.com/snapshot/0\?token=\S*', route.snapshot_url)

    with pytest.raises(UnsupportedStreamFormat):
        route.hls_url

    with pytest.raises(UnsupportedStreamFormat):
        route.mp4_url


def test_service_listing(router):
    consul = router.consul

    services = consul.get_hls_edge_services()

    assert type(services) is list
    assert len(services) > 0

    services = consul.get_mp4_edge_services()

    assert type(services) is list
    assert len(services) > 0

    services = consul.get_rtspcon_services()

    assert type(services) is list
    assert len(services) > 0

    services = consul.get_arrow_asns_services()

    assert type(services) is list
    assert len(services) > 0

    services = consul.get_mjpeg_proxy_services()

    assert type(services) is list
    assert len(services) > 0

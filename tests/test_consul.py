import asyncio
import pytest

from tests.mock_consul import MockConsul
from streamrouter.consul import ConsulClient
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
    return config


@pytest.yield_fixture
def consul_client(config, consul, event_loop):
    client = ConsulClient(config, loop=event_loop)
    event_loop.run_until_complete(client.sync())
    yield client
    client.close()


def get_services(getter, tag=None):
    return list(map(lambda s: s.id, getter(tag)))


def get_hls_edge_services(consul_client, tag=None):
    def get_load(svc):
        return round(svc.load * 1000 / svc.capacity)

    edges = consul_client.get_hls_edge_services(tag)

    return [ { "id": svc.id, "load": get_load(svc) } for svc in edges ]  # noqa


def test_update(consul_client):
    assert consul_client.is_healthy()


def test_rtspcon_services(consul_client):
    masters = get_services(consul_client.get_rtspcon_services)
    expected = [
        'rtsp-master-eu1',
        'rtsp-master-m1-eu2',
        'rtsp-master-m1-eu3',
        'rtsp-master-m2-na3',
        'rtsp-master-m3-eu2',
        'rtsp-master-m3-na3',
        'rtsp-master-m4-na3',
        'rtsp-master-na1',
        'rtsp-master-na2',
        'rtsp-master-na3',
    ]
    assert masters == expected


def test_rtspcon_tag_groups(consul_client):
    masters = get_services(consul_client.get_rtspcon_services, 'eu')
    expected = [
        'rtsp-master-eu1',
        'rtsp-master-m1-eu2',
        'rtsp-master-m1-eu3',
        'rtsp-master-m3-eu2',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_eu1')
    expected = [
        'rtsp-master-eu1',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_eu2')
    expected = [
        'rtsp-master-m1-eu2',
        'rtsp-master-m3-eu2',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_eu3')
    expected = [
        'rtsp-master-m1-eu3',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'na')
    expected = [
        'rtsp-master-m2-na3',
        'rtsp-master-m3-na3',
        'rtsp-master-m4-na3',
        'rtsp-master-na1',
        'rtsp-master-na2',
        'rtsp-master-na3',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_na1')
    expected = [
        'rtsp-master-na1',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_na2')
    expected = [
        'rtsp-master-na2',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'pop_na3')
    expected = [
        'rtsp-master-m2-na3',
        'rtsp-master-m3-na3',
        'rtsp-master-m4-na3',
        'rtsp-master-na3',
    ]
    assert masters == expected

    masters = get_services(consul_client.get_rtspcon_services, 'foo')
    assert masters == []


def test_rtspcon_node_groups(consul_client):
    masters = consul_client.get_rtspcon_services_by_node_id('m1-eu1')

    assert len(masters) == 1

    master = masters[0]

    assert master.node == 'm1-eu1'
    assert master.id == 'rtsp-master-eu1'
    assert master.host == 'm1-eu1.angelcam.com'
    assert master.healthy
    assert master.tags == ['eu', 'pop_eu1']
    assert master.url_mask == 'http://m1-eu1.angelcam.com/stream/%s/playlist.m3u8?token=%s'  # noqa


def test_rtspcon_unhealthy_services(consul, consul_client, event_loop):
    masters = consul.health['rtsp-master']
    node = masters[0]['Node']['Node']

    masters[0]['Checks'][0]['Status'] = 'failed'

    event_loop.run_until_complete(consul_client.sync())

    assert consul_client.is_healthy()

    masters = consul_client.get_rtspcon_services_by_node_id(node)

    assert len(masters) == 1

    master = masters[0]

    assert master.node == node
    assert not master.healthy


def test_hls_edge_services(consul_client):
    edges = get_hls_edge_services(consul_client)
    expected = [
        { "id": 'e1-na5.angelcam.com', "load": 268 },   # noqa
        { "id": 'e1-na4.angelcam.com', "load": 283 },   # noqa
        { "id": 'e1-na3.angelcam.com', "load": 289 },   # noqa
        { "id": 'e1-na2.angelcam.com', "load": 290 },   # noqa
        { "id": 'e2-na2.angelcam.com', "load": 303 },   # noqa
        { "id": 'e2-na3.angelcam.com', "load": 360 },   # noqa
        { "id": 'e1-eu3.angelcam.com', "load": 448 },   # noqa
        { "id": 'e3-eu1.angelcam.com', "load": 456 },   # noqa
        { "id": 'e3-eu2.angelcam.com', "load": 465 },   # noqa
        { "id": 'e1-eu1.angelcam.com', "load": 470 },   # noqa
        { "id": 'e2-eu1.angelcam.com', "load": 500 },   # noqa
        { "id": 'e4-eu2.angelcam.com', "load": 508 },   # noqa
        { "id": 'e1-eu2.angelcam.com', "load": 522 },   # noqa
    ]
    assert edges == expected


def test_hls_edge_tag_groups(consul_client):
    edges = get_hls_edge_services(consul_client, 'eu')
    expected = [
        { "id": 'e1-eu3.angelcam.com', "load": 448 },   # noqa
        { "id": 'e3-eu1.angelcam.com', "load": 456 },   # noqa
        { "id": 'e3-eu2.angelcam.com', "load": 465 },   # noqa
        { "id": 'e1-eu1.angelcam.com', "load": 470 },   # noqa
        { "id": 'e2-eu1.angelcam.com', "load": 500 },   # noqa
        { "id": 'e4-eu2.angelcam.com', "load": 508 },   # noqa
        { "id": 'e1-eu2.angelcam.com', "load": 522 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_eu1')
    expected = [
        { "id": 'e3-eu1.angelcam.com', "load": 456 },   # noqa
        { "id": 'e1-eu1.angelcam.com', "load": 470 },   # noqa
        { "id": 'e2-eu1.angelcam.com', "load": 500 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_eu2')
    expected = [
        { "id": 'e3-eu2.angelcam.com', "load": 465 },   # noqa
        { "id": 'e4-eu2.angelcam.com', "load": 508 },   # noqa
        { "id": 'e1-eu2.angelcam.com', "load": 522 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_eu3')
    expected = [
        { "id": 'e1-eu3.angelcam.com', "load": 448 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'na')
    expected = [
        { "id": 'e1-na5.angelcam.com', "load": 268 },   # noqa
        { "id": 'e1-na4.angelcam.com', "load": 283 },   # noqa
        { "id": 'e1-na3.angelcam.com', "load": 289 },   # noqa
        { "id": 'e1-na2.angelcam.com', "load": 290 },   # noqa
        { "id": 'e2-na2.angelcam.com', "load": 303 },   # noqa
        { "id": 'e2-na3.angelcam.com', "load": 360 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_na2')
    expected = [
        { "id": 'e1-na2.angelcam.com', "load": 290 },   # noqa
        { "id": 'e2-na2.angelcam.com', "load": 303 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_na3')
    expected = [
        { "id": 'e1-na3.angelcam.com', "load": 289 },   # noqa
        { "id": 'e2-na3.angelcam.com', "load": 360 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_na4')
    expected = [
        { "id": 'e1-na4.angelcam.com', "load": 283 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'pop_na5')
    expected = [
        { "id": 'e1-na5.angelcam.com', "load": 268 },   # noqa
    ]
    assert edges == expected

    edges = get_hls_edge_services(consul_client, 'foo')
    assert edges == []


def test_hls_edge_node_description(consul_client):
    edges = consul_client.get_hls_edge_services()

    assert len(edges) > 0

    edge = edges[0]

    assert edge.node == 'e1-na5'
    assert edge.id == 'e1-na5.angelcam.com'
    assert edge.host == 'e1-na5.angelcam.com'
    assert edge.healthy
    assert edge.tags == ['na', 'pop_na5']
    assert edge.load is not None
    assert edge.capacity is not None
    assert edge.url_mask == 'http://e1-na5.angelcam.com/%s/%s/playlist.m3u8?token=%s'   # noqa


def test_hls_edge_unhealthy_services(consul, consul_client, event_loop):
    consul.health['rtsp-edge'][0]['Checks'][0]['Status'] = 'failed'

    event_loop.run_until_complete(consul_client.sync())

    assert consul_client.is_healthy()
    assert len(consul_client.get_hls_edge_services()) == 12


def test_mjpeg_proxy_services(consul_client):
    proxies = get_services(consul_client.get_mjpeg_proxy_services)
    expected = [
        'mjpeg-proxy-mjpeg1-eu2-1',
        'mjpeg-proxy-mjpeg1-eu2-2',
        'mjpeg-proxy-mjpeg1-eu2-3',
        'mjpeg-proxy-mjpeg1-eu2-4',
        'mjpeg-proxy-mjpeg1-na1-1',
        'mjpeg-proxy-mjpeg1-na1-2',
        'mjpeg-proxy-mjpeg1-na1-3',
        'mjpeg-proxy-mjpeg1-na1-4',
    ]
    assert proxies == expected


def test_mjpeg_proxy_tag_groups(consul_client):
    proxies = get_services(consul_client.get_mjpeg_proxy_services, 'eu')
    expected = [
        'mjpeg-proxy-mjpeg1-eu2-1',
        'mjpeg-proxy-mjpeg1-eu2-2',
        'mjpeg-proxy-mjpeg1-eu2-3',
        'mjpeg-proxy-mjpeg1-eu2-4',
    ]
    assert proxies == expected

    proxies = get_services(consul_client.get_mjpeg_proxy_services, 'pop_eu2')
    expected = [
        'mjpeg-proxy-mjpeg1-eu2-1',
        'mjpeg-proxy-mjpeg1-eu2-2',
        'mjpeg-proxy-mjpeg1-eu2-3',
        'mjpeg-proxy-mjpeg1-eu2-4',
    ]
    assert proxies == expected

    proxies = get_services(consul_client.get_mjpeg_proxy_services, 'na')
    expected = [
        'mjpeg-proxy-mjpeg1-na1-1',
        'mjpeg-proxy-mjpeg1-na1-2',
        'mjpeg-proxy-mjpeg1-na1-3',
        'mjpeg-proxy-mjpeg1-na1-4',
    ]
    assert proxies == expected

    proxies = get_services(consul_client.get_mjpeg_proxy_services, 'pop_na1')
    expected = [
        'mjpeg-proxy-mjpeg1-na1-1',
        'mjpeg-proxy-mjpeg1-na1-2',
        'mjpeg-proxy-mjpeg1-na1-3',
        'mjpeg-proxy-mjpeg1-na1-4',
    ]
    assert proxies == expected

    proxies = get_services(consul_client.get_mjpeg_proxy_services, 'foo')
    assert proxies == []


def test_mjpeg_proxy_node_description(consul_client):
    proxies = consul_client.get_mjpeg_proxy_services()

    assert len(proxies) > 0

    proxy = proxies[0]

    assert proxy.node == 'mjpeg1-eu2'
    assert proxy.id == 'mjpeg-proxy-mjpeg1-eu2-1'
    assert proxy.host == 'mjpeg1-eu2-1.angelcam.com'
    assert proxy.healthy
    assert proxy.tags == ['eu', 'pop_eu2']
    assert proxy.url_mask == 'http://mjpeg1-eu2-1.angelcam.com/stream/%s?token=%s'  # noqa


def test_mjpeg_proxy_unhealthy_services(consul, consul_client, event_loop):
    proxies = consul.health['mjpeg-proxy']
    node = proxies[0]['Node']['Node']

    proxies[0]['Checks'][0]['Status'] = 'failed'

    event_loop.run_until_complete(consul_client.sync())

    assert consul_client.is_healthy()

    proxies = consul_client.get_mjpeg_proxy_services()

    assert len(proxies) > 0

    proxy = proxies[0]

    assert proxy.node == node
    assert not proxy.healthy


def test_arrow_asns_services(consul_client):
    services = get_services(consul_client.get_arrow_asns_services)
    expected = [
        'arrow-asns-eu1',
        'arrow-asns-m1-eu2',
        'arrow-asns-m1-eu3',
        'arrow-asns-m2-na3',
        'arrow-asns-m3-eu2',
        'arrow-asns-m3-na3',
        'arrow-asns-m4-na3',
        'arrow-asns-na1',
        'arrow-asns-na2',
        'arrow-asns-na3',
    ]
    assert services == expected


def test_arrow_asns_tag_groups(consul_client):
    services = get_services(consul_client.get_arrow_asns_services, 'eu')
    expected = [
        'arrow-asns-eu1',
        'arrow-asns-m1-eu2',
        'arrow-asns-m1-eu3',
        'arrow-asns-m3-eu2',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_eu1')
    expected = [
        'arrow-asns-eu1',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_eu2')
    expected = [
        'arrow-asns-m1-eu2',
        'arrow-asns-m3-eu2',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_eu3')
    expected = [
        'arrow-asns-m1-eu3',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'na')
    expected = [
        'arrow-asns-m2-na3',
        'arrow-asns-m3-na3',
        'arrow-asns-m4-na3',
        'arrow-asns-na1',
        'arrow-asns-na2',
        'arrow-asns-na3',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_na1')
    expected = [
        'arrow-asns-na1',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_na2')
    expected = [
        'arrow-asns-na2',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'pop_na3')
    expected = [
        'arrow-asns-m2-na3',
        'arrow-asns-m3-na3',
        'arrow-asns-m4-na3',
        'arrow-asns-na3',
    ]
    assert services == expected

    services = get_services(consul_client.get_arrow_asns_services, 'foo')
    assert services == []


def test_arrow_asns_node_description(consul_client):
    services = consul_client.get_arrow_asns_services()

    assert len(services) > 0

    service = services[0]

    assert service.node == 'm1-eu1'
    assert service.id == 'arrow-asns-eu1'
    assert service.host == 'm1-eu1.angelcam.com'
    assert service.healthy
    assert service.tags == ['eu', 'pop_eu1']
    assert service.services['rtsp_proxy'] == 'm1-eu1.angelcam.com:8920'
    assert service.services['http_proxy'] == 'm1-eu1.angelcam.com:8921'


def test_arrow_asns_unhealthy_services(consul, consul_client, event_loop):
    services = consul.health['arrow-asns']
    node = services[0]['Node']['Node']

    services[0]['Checks'][0]['Status'] = 'failed'

    event_loop.run_until_complete(consul_client.sync())

    assert consul_client.is_healthy()

    services = consul_client.get_arrow_asns_services()

    assert len(services) > 0

    service = services[0]

    assert service.node == node
    assert not service.healthy

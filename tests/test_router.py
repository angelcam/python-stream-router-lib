import asyncio
import math
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
    event_loop.run_until_complete(router.sync())
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

    event_loop.run_until_complete(router.sync())

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

    event_loop.run_until_complete(router.sync())

    assert router.is_healthy()


def remove_by_tag(service, tag, consul, router, event_loop):
    services = consul.health.get(service)
    if not services:
        return

    services = filter(lambda svc: tag not in svc['Service']['Tags'], services)

    consul.health[service] = list(services)

    event_loop.run_until_complete(router.sync())

    assert router.is_healthy()


def remove_all(service, consul, router, event_loop):
    consul.health[service] = []
    event_loop.run_until_complete(router.sync())
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
    event_loop.run_until_complete(router.sync())

    assert router.is_healthy()

    region = 'eu'
    resource = Resource(
        16,
        setup=Resource.ARROW,
        arrow_uuid='00000011')

    route = router.construct_rtspcon_route(region, resource)

    assert route is None


def get_rtspcon_routes(router, region, resources):
    routes = {}

    for resource in resources:
        route = router.construct_rtspcon_route(region, resource)
        if not route:
            raise Exception("unexpected routing fail")

        if route.master.node in routes:
            routes[route.master.node]['count'] += 1
        else:
            routes[route.master.node] = {
                "service": route.master,
                "count": 1
            }

    return routes


def get_service_share(routes):
    service = routes['service']
    count = routes['count']
    return count / service.capacity


def get_relative_standard_deviation(routes):
    shares = list(map(get_service_share, routes))
    mean = sum(shares) / len(shares)
    sdeltas = map(lambda s: (s - mean) ** 2, shares)
    variance = sum(sdeltas) / (len(shares) - 1)
    deviation = math.sqrt(variance)
    return deviation / mean


def test_routing_distribution(router):
    region = 'eu'

    resources = map(lambda camera_id: Resource(camera_id), range(10000))
    routes = get_rtspcon_routes(router, region, resources)

    assert len(routes) == 4

    reldev = get_relative_standard_deviation(routes.values())

    assert reldev < 0.05


def test_routing_distribution_with_unhealthy_node(consul, router, event_loop):
    set_healthy('rtsp-master', 'm1-eu1', False, consul, router, event_loop)

    region = 'eu'

    resources = map(lambda camera_id: Resource(camera_id), range(10000))
    routes = get_rtspcon_routes(router, region, resources)

    assert len(routes) == 3

    reldev = get_relative_standard_deviation(routes.values())

    assert reldev < 0.05


test_data = [
    {"camera_id": "6460", "setup": "common"},  # noqa
    {"camera_id": "26530", "setup": "common"},  # noqa
    {"camera_id": "2868", "setup": "common"},  # noqa
    {"camera_id": "24750", "setup": "common"},  # noqa
    {"camera_id": "14516", "setup": "common"},  # noqa
    {"camera_id": "19902", "setup": "common"},  # noqa
    {"camera_id": "26085", "setup": "common"},  # noqa
    {"camera_id": "5632", "setup": "common"},  # noqa
    {"camera_id": "16941", "setup": "common"},  # noqa
    {"camera_id": "13016", "setup": "common"},  # noqa
    {"camera_id": "15344", "setup": "common"},  # noqa
    {"camera_id": "20373", "setup": "common"},  # noqa
    {"camera_id": "13566", "setup": "common"},  # noqa
    {"camera_id": "7488", "setup": "common"},  # noqa
    {"camera_id": "17083", "setup": "common"},  # noqa
    {"camera_id": "13294", "setup": "common"},  # noqa
    {"camera_id": "8968", "setup": "common"},  # noqa
    {"camera_id": "3587", "setup": "common"},  # noqa
    {"camera_id": "25950", "setup": "common"},  # noqa
    {"camera_id": "22651", "setup": "common"},  # noqa
    {"camera_id": "412", "setup": "common"},  # noqa
    {"camera_id": "4525", "setup": "common"},  # noqa
    {"camera_id": "25445", "setup": "common"},  # noqa
    {"camera_id": "7978", "setup": "common"},  # noqa
    {"camera_id": "9493", "setup": "common"},  # noqa
    {"camera_id": "733", "setup": "common"},  # noqa
    {"camera_id": "23083", "setup": "common"},  # noqa
    {"camera_id": "19564", "setup": "common"},  # noqa
    {"camera_id": "25988", "setup": "common"},  # noqa
    {"camera_id": "11630", "setup": "common"},  # noqa
    {"camera_id": "14480", "setup": "common"},  # noqa
    {"camera_id": "12403", "setup": "common"},  # noqa
    {"camera_id": "9754", "setup": "common"},  # noqa
    {"camera_id": "569", "setup": "common"},  # noqa
    {"camera_id": "7008", "setup": "common"},  # noqa
    {"camera_id": "3814", "setup": "common"},  # noqa
    {"camera_id": "1127", "setup": "common"},  # noqa
    {"camera_id": "1688", "setup": "common"},  # noqa
    {"camera_id": "9096", "setup": "common"},  # noqa
    {"camera_id": "13114", "setup": "common"},  # noqa
    {"camera_id": "5482", "setup": "common"},  # noqa
    {"camera_id": "17220", "setup": "common"},  # noqa
    {"camera_id": "15267", "setup": "common"},  # noqa
    {"camera_id": "1819", "setup": "common"},  # noqa
    {"camera_id": "26565", "setup": "common"},  # noqa
    {"camera_id": "13193", "setup": "common"},  # noqa
    {"camera_id": "26067", "setup": "common"},  # noqa
    {"camera_id": "13179", "setup": "common"},  # noqa
    {"camera_id": "18128", "setup": "common"},  # noqa
    {"camera_id": "8972", "setup": "common"},  # noqa
    {"camera_id": "6395", "setup": "common"},  # noqa
    {"camera_id": "21940", "setup": "common"},  # noqa
    {"camera_id": "3319", "setup": "common"},  # noqa
    {"camera_id": "19930", "setup": "common"},  # noqa
    {"camera_id": "11614", "setup": "common"},  # noqa
    {"camera_id": "8952", "setup": "common"},  # noqa
    {"camera_id": "6308", "setup": "common"},  # noqa
    {"camera_id": "11219", "setup": "common"},  # noqa
    {"camera_id": "16302", "setup": "common"},  # noqa
    {"camera_id": "25741", "setup": "common"},  # noqa
    {"camera_id": "1863", "setup": "common"},  # noqa
    {"camera_id": "25121", "setup": "common"},  # noqa
    {"camera_id": "12095", "setup": "common"},  # noqa
    {"camera_id": "26904", "setup": "common"},  # noqa
    {"camera_id": "23640", "setup": "common"},  # noqa
    {"camera_id": "678", "setup": "common"},  # noqa
    {"camera_id": "5365", "setup": "common"},  # noqa
    {"camera_id": "26065", "setup": "common"},  # noqa
    {"camera_id": "25929", "setup": "common"},  # noqa
    {"camera_id": "9352", "setup": "common"},  # noqa
    {"camera_id": "20842", "setup": "common"},  # noqa
    {"camera_id": "24987", "setup": "common"},  # noqa
    {"camera_id": "15252", "setup": "common"},  # noqa
    {"camera_id": "14312", "setup": "common"},  # noqa
    {"camera_id": "1392", "setup": "common"},  # noqa
    {"camera_id": "21022", "setup": "common"},  # noqa
    {"camera_id": "7004", "setup": "common"},  # noqa
    {"camera_id": "4916", "setup": "common"},  # noqa
    {"camera_id": "2877", "setup": "common"},  # noqa
    {"camera_id": "24860", "setup": "common"},  # noqa
    {"camera_id": "6911", "setup": "common"},  # noqa
    {"camera_id": "23589", "setup": "common"},  # noqa
    {"camera_id": "25725", "setup": "common"},  # noqa
    {"camera_id": "17197", "setup": "common"},  # noqa
    {"camera_id": "7435", "setup": "common"},  # noqa
    {"camera_id": "8614", "setup": "common"},  # noqa
    {"camera_id": "14572", "setup": "common"},  # noqa
    {"camera_id": "17110", "setup": "common"},  # noqa
    {"camera_id": "26829", "setup": "common"},  # noqa
    {"camera_id": "791", "setup": "common"},  # noqa
    {"camera_id": "15726", "setup": "common"},  # noqa
    {"camera_id": "11318", "setup": "common"},  # noqa
    {"camera_id": "19518", "setup": "common"},  # noqa
    {"camera_id": "22551", "setup": "common"},  # noqa
    {"camera_id": "23186", "setup": "common"},  # noqa
    {"camera_id": "19830", "setup": "common"},  # noqa
    {"camera_id": "574", "setup": "common"},  # noqa
    {"camera_id": "25494", "setup": "common"},  # noqa
    {"camera_id": "5641", "setup": "common"},  # noqa
    {"camera_id": "24409", "setup": "common"},  # noqa
    {"arrow_uuid": "2afb5248-3e1b-4772-8b87-3ec0d61f9777", "camera_id": "21673", "setup": "arrow"},  # noqa
    {"arrow_uuid": "df6cff59-b639-47a8-b8e7-54d4c3de6dc5", "camera_id": "29058", "setup": "arrow"},  # noqa
    {"arrow_uuid": "5a833343-60af-4743-a23a-a8742ac12856", "camera_id": "19867", "setup": "arrow"},  # noqa
    {"arrow_uuid": "d4ada610-73ae-4723-8ef2-aab03179885a", "camera_id": "8620", "setup": "arrow"},  # noqa
    {"arrow_uuid": "a2d51a65-377e-4335-b370-7f188bb891b4", "camera_id": "9268", "setup": "arrow"},  # noqa
    {"arrow_uuid": "060c5eb1-fb5e-4885-a84a-73dadd077b52", "camera_id": "13592", "setup": "arrow"},  # noqa
    {"arrow_uuid": "8adb3932-79b5-4b9b-a775-24a0ed7500fc", "camera_id": "23011", "setup": "arrow"},  # noqa
    {"arrow_uuid": "a0e9f5ac-db01-4dea-835f-69fd00a8e17e", "camera_id": "29437", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c9788c7e-a470-45eb-af52-9748ab4ec837", "camera_id": "14703", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6c125a7f-c611-4d19-9ead-fc2ecda569e1", "camera_id": "9204", "setup": "arrow"},  # noqa
    {"arrow_uuid": "49c7b844-56a2-4c89-9c44-c17102f9ee87", "camera_id": "468", "setup": "arrow"},  # noqa
    {"arrow_uuid": "46bd6726-c44f-4ca9-b16b-8e651fdb54b6", "camera_id": "26113", "setup": "arrow"},  # noqa
    {"arrow_uuid": "214974f4-f920-47ac-957f-7446609411df", "camera_id": "2677", "setup": "arrow"},  # noqa
    {"arrow_uuid": "639b93cd-952f-457d-acea-4bd6f7005439", "camera_id": "8232", "setup": "arrow"},  # noqa
    {"arrow_uuid": "54e2733a-2170-4cf5-b938-9c68853ab807", "camera_id": "17116", "setup": "arrow"},  # noqa
    {"arrow_uuid": "17e75277-4d67-4410-80b6-3212aa2f891a", "camera_id": "20931", "setup": "arrow"},  # noqa
    {"arrow_uuid": "0fa5f02f-0e48-4d48-9968-3638eb927002", "camera_id": "10395", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c043e464-aedc-4fac-8d60-739adfd05284", "camera_id": "26233", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cd0ade4a-7a9e-4a09-82fd-2868d9abe50a", "camera_id": "27498", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6de041fe-a441-489a-853c-4ba7f9a36f9a", "camera_id": "7129", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c3c2986e-2c5e-4337-93cc-610a4f58ae2f", "camera_id": "19533", "setup": "arrow"},  # noqa
    {"arrow_uuid": "d0ba88dd-1904-4ba4-a571-7288dbdf082f", "camera_id": "20965", "setup": "arrow"},  # noqa
    {"arrow_uuid": "2a8707bc-02fa-402d-b012-6457b2c4622b", "camera_id": "19853", "setup": "arrow"},  # noqa
    {"arrow_uuid": "1ddc5ae2-bff1-42cb-b825-b66f7f06428a", "camera_id": "18049", "setup": "arrow"},  # noqa
    {"arrow_uuid": "f636ea62-2cc3-4d86-8281-9e03f43e8502", "camera_id": "12905", "setup": "arrow"},  # noqa
    {"arrow_uuid": "1daceee2-2a8f-4444-bb6d-768d54c15d55", "camera_id": "20261", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ff52264c-bc35-4706-9f0a-5a80d82ee298", "camera_id": "24396", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6cd72cd2-d2d0-4d25-a050-19129163fb65", "camera_id": "12949", "setup": "arrow"},  # noqa
    {"arrow_uuid": "e0facd7f-d0dc-4ceb-a097-6569c10a76b4", "camera_id": "24912", "setup": "arrow"},  # noqa
    {"arrow_uuid": "5c2388a2-df28-44d6-80f1-5d5691dcde9d", "camera_id": "5365", "setup": "arrow"},  # noqa
    {"arrow_uuid": "84500cf9-63e2-4e76-b635-c4915ede1b9e", "camera_id": "18422", "setup": "arrow"},  # noqa
    {"arrow_uuid": "da3d628b-1af4-44b7-8349-3bbac375d3ba", "camera_id": "3166", "setup": "arrow"},  # noqa
    {"arrow_uuid": "b61f82a6-81b7-46bb-a6af-23ec2d5e2f01", "camera_id": "912", "setup": "arrow"},  # noqa
    {"arrow_uuid": "33a0a7d8-dd44-433a-9baa-060c8691e804", "camera_id": "3770", "setup": "arrow"},  # noqa
    {"arrow_uuid": "0ebe874a-7b46-47c1-8a2d-847f40f6b346", "camera_id": "16314", "setup": "arrow"},  # noqa
    {"arrow_uuid": "22051a73-1528-4dfa-bd6d-90016a418fa7", "camera_id": "26399", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ff5aaba4-9f55-42fe-8b0d-a3de10e61ea5", "camera_id": "3075", "setup": "arrow"},  # noqa
    {"arrow_uuid": "fc4ae671-ec44-41cb-90b9-d40186767521", "camera_id": "5324", "setup": "arrow"},  # noqa
    {"arrow_uuid": "4be3063c-f557-41c6-8da9-5aee3b72b396", "camera_id": "11356", "setup": "arrow"},  # noqa
    {"arrow_uuid": "2b997ced-4a1d-4192-b9c2-e6aa39f6bb74", "camera_id": "27435", "setup": "arrow"},  # noqa
    {"arrow_uuid": "84df8f10-2638-4a84-a288-0921eeeb4986", "camera_id": "19887", "setup": "arrow"},  # noqa
    {"arrow_uuid": "bae8e69d-1281-4a65-ba3d-016638d203f9", "camera_id": "7630", "setup": "arrow"},  # noqa
    {"arrow_uuid": "f42e1f89-c8c7-4a95-87e5-551aeec5f4c3", "camera_id": "22376", "setup": "arrow"},  # noqa
    {"arrow_uuid": "242c9386-e24c-4996-bee5-3c9cc04ee188", "camera_id": "13038", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cfe0b9b4-b00e-45fb-878e-9bb8a2a0d090", "camera_id": "4511", "setup": "arrow"},  # noqa
    {"arrow_uuid": "55cf53e0-9e65-4387-806c-729b6787564d", "camera_id": "7826", "setup": "arrow"},  # noqa
    {"arrow_uuid": "98e7bb56-5966-4451-a092-9fae08768a0c", "camera_id": "12320", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ff9310b7-0f3f-4494-ab0c-539246b1fe8a", "camera_id": "29033", "setup": "arrow"},  # noqa
    {"arrow_uuid": "32d63f71-2603-4afb-bc9e-855d2dce5c25", "camera_id": "24993", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ebb23fbb-8905-4d55-9f40-ce9e9ab2d961", "camera_id": "3523", "setup": "arrow"},  # noqa
    {"arrow_uuid": "110a7807-c16b-4fb0-b851-be88e9546f09", "camera_id": "27265", "setup": "arrow"},  # noqa
    {"arrow_uuid": "85c36cdd-505b-493d-90c8-ba2a0c4e3e4a", "camera_id": "19911", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c40187bd-a997-4414-9e13-44710c56689a", "camera_id": "18694", "setup": "arrow"},  # noqa
    {"arrow_uuid": "abb9ecde-d980-4b8a-8533-7030993d0e00", "camera_id": "1165", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ea753ef2-0bd3-470f-bec8-2ca8090312bd", "camera_id": "12332", "setup": "arrow"},  # noqa
    {"arrow_uuid": "adb8e0f1-119a-4950-b2b5-252f692ec881", "camera_id": "21494", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6c3d4cb1-c115-44e8-bc54-86191017d199", "camera_id": "4890", "setup": "arrow"},  # noqa
    {"arrow_uuid": "026034ba-f5b1-480c-8def-c7d9061151b5", "camera_id": "29961", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cccaaeb0-706c-440a-a7c8-321a6dd9ea17", "camera_id": "26162", "setup": "arrow"},  # noqa
    {"arrow_uuid": "8391d5b8-2571-4090-aa11-921ad0be7a49", "camera_id": "25975", "setup": "arrow"},  # noqa
    {"arrow_uuid": "b72c36ac-3ca0-4531-bcd5-c2121a1abfde", "camera_id": "12086", "setup": "arrow"},  # noqa
    {"arrow_uuid": "8cd9baa1-ed40-4a86-9ecd-3a2f6cd186dc", "camera_id": "22870", "setup": "arrow"},  # noqa
    {"arrow_uuid": "819c2c66-ba15-4478-9158-6c3450410c1b", "camera_id": "21904", "setup": "arrow"},  # noqa
    {"arrow_uuid": "2f8ae143-abe1-49d8-944b-67bd3b7a4d48", "camera_id": "21242", "setup": "arrow"},  # noqa
    {"arrow_uuid": "835413f1-7964-4444-8365-0bb34cced483", "camera_id": "13822", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6748fed4-2cd3-4f41-8a97-ec43b8e5f89c", "camera_id": "3899", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cd798626-02a5-4d74-b4c7-281be8995829", "camera_id": "25251", "setup": "arrow"},  # noqa
    {"arrow_uuid": "a947b83b-8d63-46aa-9210-8e7717494d42", "camera_id": "17739", "setup": "arrow"},  # noqa
    {"arrow_uuid": "a70e6638-b80e-4ccd-a3eb-3fc22c2094a3", "camera_id": "25912", "setup": "arrow"},  # noqa
    {"arrow_uuid": "3213d804-9069-48d6-8a4e-7545a660f9a3", "camera_id": "15589", "setup": "arrow"},  # noqa
    {"arrow_uuid": "54243dcb-7e0a-4949-a801-cfc4d1491e48", "camera_id": "16939", "setup": "arrow"},  # noqa
    {"arrow_uuid": "84e2df83-5c3b-489d-aa8c-b84bacebe496", "camera_id": "10582", "setup": "arrow"},  # noqa
    {"arrow_uuid": "a930de31-3aab-4850-9d64-b0a87875aee6", "camera_id": "20924", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c2280e58-7439-4573-9758-9b6b2b588800", "camera_id": "29487", "setup": "arrow"},  # noqa
    {"arrow_uuid": "c425cfc7-138e-42a9-b915-8853be35c7d8", "camera_id": "2210", "setup": "arrow"},  # noqa
    {"arrow_uuid": "eb51442a-0fd0-4c9e-949f-ff5ec8031591", "camera_id": "17566", "setup": "arrow"},  # noqa
    {"arrow_uuid": "683d1088-67cc-4f54-a669-24bc56136142", "camera_id": "10682", "setup": "arrow"},  # noqa
    {"arrow_uuid": "f648f641-910d-4e2c-a1ce-3be384f097ab", "camera_id": "167", "setup": "arrow"},  # noqa
    {"arrow_uuid": "1f0e29af-6d6c-4b58-b3e4-e1cca3299b5f", "camera_id": "29364", "setup": "arrow"},  # noqa
    {"arrow_uuid": "be92aac8-57ab-4de2-aab3-c4a1df23b303", "camera_id": "17520", "setup": "arrow"},  # noqa
    {"arrow_uuid": "bc25a6aa-3962-4364-8678-a75aeaaa79f6", "camera_id": "9986", "setup": "arrow"},  # noqa
    {"arrow_uuid": "f1f1480d-6fe2-4a0f-990d-c8f6e5afc433", "camera_id": "4335", "setup": "arrow"},  # noqa
    {"arrow_uuid": "b1c74ea3-1519-4dfb-9f69-f86c01c1ecd9", "camera_id": "14409", "setup": "arrow"},  # noqa
    {"arrow_uuid": "4149addc-8bf9-4067-8016-0375c7d47535", "camera_id": "24578", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cb612acc-ce74-47c0-ab8b-623ddbd07161", "camera_id": "23605", "setup": "arrow"},  # noqa
    {"arrow_uuid": "0463d078-a663-4a3f-b185-a977ea973b5d", "camera_id": "7387", "setup": "arrow"},  # noqa
    {"arrow_uuid": "6b6ee679-762a-4cf1-aafc-705de9aeb0a9", "camera_id": "24958", "setup": "arrow"},  # noqa
    {"arrow_uuid": "1d94e2be-f74b-4476-8241-31973bd4f89d", "camera_id": "19393", "setup": "arrow"},  # noqa
    {"arrow_uuid": "d91957dd-4e27-44ae-a29b-0511ec1ed0d7", "camera_id": "25298", "setup": "arrow"},  # noqa
    {"arrow_uuid": "e2eea5a7-6fb7-4963-9d0f-a2f3366a3ab1", "camera_id": "4430", "setup": "arrow"},  # noqa
    {"arrow_uuid": "cbb026fc-4788-4a9e-a603-526ec6001973", "camera_id": "12535", "setup": "arrow"},  # noqa
    {"arrow_uuid": "2bd877fd-f591-4244-8bcb-d1c72492ef92", "camera_id": "10787", "setup": "arrow"},  # noqa
    {"arrow_uuid": "072be713-3fde-48bd-acf1-40147830f211", "camera_id": "7661", "setup": "arrow"},  # noqa
    {"arrow_uuid": "65cff9d7-9f2d-4d5e-bf44-523c3fc090eb", "camera_id": "16638", "setup": "arrow"},  # noqa
    {"arrow_uuid": "33380d59-a50f-43dc-aa92-90c4b8e8e727", "camera_id": "14865", "setup": "arrow"},  # noqa
    {"arrow_uuid": "670f0c57-20e1-49bc-878f-45f4578764ed", "camera_id": "5748", "setup": "arrow"},  # noqa
    {"arrow_uuid": "80b8186c-d8a4-49ac-bb21-92c32a06e2ae", "camera_id": "23048", "setup": "arrow"},  # noqa
    {"arrow_uuid": "ce5e63e2-8177-4bd9-887b-95543120a4d6", "camera_id": "21627", "setup": "arrow"},  # noqa
    {"arrow_uuid": "75f7119b-71d9-47ac-840c-1efba121c291", "camera_id": "24134", "setup": "arrow"},  # noqa
    {"arrow_uuid": "4311a844-0ce0-43fa-8225-d04bf371737b", "camera_id": "17929", "setup": "arrow"}  # noqa
]


def test_routing_comparison(router):
    expected = [
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu2",
        "m1-eu1",
        "m1-eu2",
        "m3-eu2",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m3-eu2",
        "m1-eu1",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu2",
        "m1-eu1",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m3-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m1-eu1",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu1",
        "m1-eu1",
        "m1-eu2",
        "m1-eu2",
        "m1-eu1",
        "m1-eu2",
        "m1-eu1",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m3-eu2",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m3-eu2",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu2",
        "m1-eu1",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu1",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu1",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu3",
        "m1-eu1",
        "m1-eu2",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu1",
        "m1-eu2",
        "m1-eu3",
        "m3-eu2"
    ]

    region = 'eu'

    result = []

    for resource in test_data:
        resource = Resource(
            resource.get('camera_id'),
            setup=resource.get('setup'),
            arrow_uuid=resource.get('arrow_uuid'))

        route = router.construct_rtspcon_route(region, resource)
        if not route:
            raise Exception("unexpected routing fail")

        result.append(route.master.node)

    assert result == expected


def test_routing_comparison_with_unhealthy_node(consul, router, event_loop):
    expected = [
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m3-eu2",
        "m1-eu3",
        "m1-eu3",
        "m3-eu2",
        "m1-eu2",
        "m1-eu3",
        "m1-eu2",
        "m1-eu2",
        "m1-eu3",
        "m3-eu2"
    ]

    set_healthy('rtsp-master', 'm1-eu1', False, consul, router, event_loop)

    region = 'eu'

    result = []

    for resource in test_data:
        resource = Resource(
            resource.get('camera_id'),
            setup=resource.get('setup'),
            arrow_uuid=resource.get('arrow_uuid'))

        route = router.construct_rtspcon_route(region, resource)
        if not route:
            raise Exception("unexpected routing fail")

        result.append(route.master.node)

    assert result == expected

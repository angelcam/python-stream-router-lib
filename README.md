# Stream router library

Simple library intended to group all stream routing algorithms used throughout
the CDN.

## Installation

Add the following line into you `requirements.txt`:

```
git+https://github.com/angelcam/python-stream-router-lib.git@v5.2.0#egg=streamrouter
```

and run `pip3 install -r requirements.txt`.

## Usage

Here is a simple usage example:

```Python
from streamrouter import StreamRouter, Camera
from streamrouter import RouterConfig

...

# stream router configuration
config = RouterConfig()

config.consul_host = 'consul'
config.consul_port = 8500
config.rtspcon_secret = 'secret'
config.mjpeg_proxy_secret = 'secret'

# create a new stream router
router = StreamRouter(config)

# it might be a good idea to log exceptions ;)
# you can use the event to wait until the router gets initialized
def updated():
    ex = router.exception()
    if ex is not None:
        print(ex)

router.add_update_callback(updated)

# get rtspcon service for a given camera
master = router.assign_rtspcon_service('eu', Camera(5))

# get rtspcon service for a given Arrow camera
master = router.assign_rtspcon_service('eu', Camera(
    '10',
    setup=Camera.ARROW,
    arrow_uuid='1234567890abcdef'
))

# get Arrow ASNS service for a given Arrow client
asns = router.assign_arrow_asns_service('eu', '1234567890abcdef')

...

# get HLS edge URL for a given camera
route = router.construct_edge_route('eu', Camera(
    'preview-10',
    setup=Camera.ARROW,
    arrow_uuid='1234567890abcdef'),
)
url = route.hls_url

# get rtsp_con URL for a given camera

route = router.construct_rtspcon_route('eu', Camera(
    '10',
    setup=Camera.ARROW,
    arrow_uuid='1234567890abcdef')
)
route.hls_url
route.mp4_url
route.mjpeg_url
route.snapshot_url
```

See the `StreamRouter`, `Camera` and `XXXRoute` docs for all available
methods and fields. The `assign_xxx_service()` methods return instances of the
`XXXService` class. The `construct_XXX_route()` methods return instances of the
`XXXRoute` classes.

**Important note:** the stream router does not re-map continent codes as the
RTSP router does. Routing decision will be always made within a given region
(if possible).

## Development

- Before each commit, make sure that all tests are passing and that there are
  no syntax errors. Use the the following commands: `py.test` and
  `flake8`.
- In order to release a new version, change the package version in
  `setup.py` and tag the version in the git repository.

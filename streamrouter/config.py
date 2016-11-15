class RouterConfig(object):

    consul_host = 'consul'
    consul_port = 8500

    sync_period = 600

    rtspcon_secret = None
    rtspcon_ttl = 3600

    mjpeg_proxy_secret = None
    mjpeg_proxy_ttl = 120

    arrow_asns_port = 8901
    arrow_asns_api_port = 8910
    arrow_asns_rtsp_proxy_port = 8920
    arrow_asns_http_proxy_port = 8921

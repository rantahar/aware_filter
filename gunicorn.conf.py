bind = "0.0.0.0:3446"
workers = 2
worker_class = "sync"
timeout = 120
keepalive = 2
max_requests = 1000
max_requests_jitter = 50
preload_app = True

# SSL/TLS
certfile = "/tmp/cert.pem"
keyfile = "/tmp/key.pem"
ssl_version = 5  # TLS 1.2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
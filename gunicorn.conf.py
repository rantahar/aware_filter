bind = "0.0.0.0:3446"
workers = 2
worker_class = "sync"
timeout = 120
keepalive = 2
max_requests = 1000
max_requests_jitter = 50
preload_app = True

# SSL/TLS with self-signed certificate
certfile = "/etc/ssl/aware-filter/cert.pem"
keyfile = "/etc/ssl/aware-filter/key.pem"
# keyfile = "/path/to/your/private.key"
# ssl_version = 5  # TLS 1.2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "debug"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'
bind = "0.0.0.0:3446"
workers = 2
worker_class = "sync"
timeout = 300  # Increased timeout for very large datasets
keepalive = 2
max_requests = 500  # Lower max requests to recycle workers more frequently
graceful_timeout = 30  # Time to gracefully shutdown workers
max_requests_jitter = 50
preload_app = True
worker_connections = 1000

# Memory management
max_worker_memory = 512  # MB - restart worker if it exceeds this
worker_tmp_dir = "/tmp"

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
bind = "0.0.0.0:3446"
workers = 2
worker_class = "sync"
timeout = 120
keepalive = 2
max_requests = 1000
max_requests_jitter = 50
preload_app = True

# SSL/TLS - Update these paths to your actual certificate locations
# Example locations:
# certfile = "/etc/ssl/certs/aware-filter.crt"
# keyfile = "/etc/ssl/private/aware-filter.key"
# Or for Let's Encrypt:
# certfile = "/etc/letsencrypt/live/yourdomain.com/fullchain.pem"
# keyfile = "/etc/letsencrypt/live/yourdomain.com/privkey.pem"

# For now, commenting out SSL to run on HTTP
# Uncomment and update paths when you have proper certificates
# certfile = "/path/to/your/certificate.pem"
# keyfile = "/path/to/your/private.key"
# ssl_version = 5  # TLS 1.2

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
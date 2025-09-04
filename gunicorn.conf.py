import os

workers = int(os.getenv("WEB_CONCURRENCY", "4"))
worker_class = "uvicorn.workers.UvicornWorker"
bind = "0.0.0.0:8000"
timeout = int(os.getenv("WEB_TIMEOUT", "240"))
graceful_timeout = 30
keepalive = int(os.getenv("WEB_KEEPALIVE", "5"))
threads = int(os.getenv("WEB_THREADS", "8"))
accesslog = "-"
errorlog = "-"
loglevel = "info"

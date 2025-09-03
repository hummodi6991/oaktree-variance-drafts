# Gunicorn tuned for Azure App Service to avoid 502s on longer requests
import multiprocessing

workers = max(2, multiprocessing.cpu_count() // 2)
threads = 4
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 240  # seconds; keep under Azure front-end 230-240 window
graceful_timeout = 30
keepalive = 5
loglevel = "info"


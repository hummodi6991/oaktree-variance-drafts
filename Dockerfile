FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
# Install system deps if you need them; keep minimal
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt && pip install gunicorn
# App code
COPY . /app
# App Service passes $PORT; expose a default dev port for local runs
ENV PORT=8000
# Configure your entrypoint via APP_MODULE (defaults to app.main:app). Example: "main:app" or "projectname.wsgi"
ENV APP_MODULE=app.main:app
# Start via gunicorn; override APP_MODULE in App Settings if your module is different
CMD ["sh", "-c", "gunicorn -b 0.0.0.0:${PORT} ${APP_MODULE}"]

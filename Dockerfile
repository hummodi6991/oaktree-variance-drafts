FROM python:3.11-slim
WORKDIR /app

# Install Python deps from the project's manifest (includes pandas, openpyxl, pdf libs, etc.)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source after deps for better Docker layer caching
COPY app /app/app
COPY README.md /app/README.md
COPY gunicorn.conf.py /app/gunicorn.conf.py

EXPOSE 8000
ENV PYTHONUNBUFFERED=1

# Use gunicorn in containers (keeps parity with provided config)
CMD [ "gunicorn", "-c", "gunicorn.conf.py", "app.main:app" ]

FROM python:3.11-slim
WORKDIR /app
COPY app /app/app
COPY README.md /app/README.md
RUN pip install --no-cache-dir fastapi uvicorn pydantic openai
EXPOSE 8000
ENV PYTHONUNBUFFERED=1
CMD [ "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000" ]

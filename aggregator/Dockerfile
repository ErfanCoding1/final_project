FROM python:3.9-slim

WORKDIR /app


RUN pip install --no-cache-dir fastapi uvicorn pydantic

COPY aggregator.py .

CMD ["python", "aggregator.py"]

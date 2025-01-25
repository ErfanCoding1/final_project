FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn websockets requests pydantic

COPY signal-generator.py .

CMD ["python", "signal-generator.py"]

FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas "psycopg[binary]>=3.3.4" fastapi "uvicorn[standard]"

COPY ingestion/ ingestion/

EXPOSE 8000

ENV CSV_SERVICE_HOST=0.0.0.0
ENV CSV_SERVICE_PORT=8000

CMD ["python", "-m", "ingestion.service"]

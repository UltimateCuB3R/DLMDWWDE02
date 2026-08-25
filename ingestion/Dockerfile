FROM python:3.11

RUN pip install --upgrade pip

WORKDIR /app/src

COPY requirements.txt ./requirements.txt
COPY ingest.py ./ingest.py
# COPY app.py ./app.py
# COPY table_definition.xml ./table_definition.xml
ADD game_data.tar.gz /app/data/
RUN pip install -r ./requirements.txt

RUN chmod 777 /app/src/
RUN chmod 777 /app/data/

EXPOSE 8000

CMD ["uvicorn", "ingestion.app:app", "--host", "0.0.0.0", "--port", "8000"]

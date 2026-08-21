FROM python:3.11

RUN pip install --upgrade pip

WORKDIR /app/src

COPY requirements.txt ./requirements.txt
COPY ingest.py ./ingest.py
RUN pip install -r ./requirements.txt

RUN chmod 777 /app/src/

CMD ["python", "ingest.py"]
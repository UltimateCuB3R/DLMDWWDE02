FROM python:3.11

RUN pip install --upgrade pip

WORKDIR /app/src

COPY requirements.txt ./requirements.txt
COPY replay.py ./replay.py
RUN pip install -r ./requirements.txt

RUN chmod 777 /app/src/

EXPOSE 8001

CMD ["uvicorn", "replay:app", "--host", "0.0.0.0", "--port", "8001"]

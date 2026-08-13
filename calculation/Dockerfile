FROM apache/flink:2.1.0-java21
ARG FLINK_VERSION=2.1.0

COPY requirements.txt /tmp/requirements.txt
COPY calc.py /opt/flink/usrlib/calc.py
COPY table_definition.xml /opt/flink/usrlib/table_definition.xml

# Install python3.12 and pyflink
RUN set -ex; \
  apt-get update && \
  apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev lzma liblzma-dev && \
  wget https://www.python.org/ftp/python/3.12.13/Python-3.12.13.tgz && \
  tar -xvf Python-3.12.13.tgz && \
  cd Python-3.12.13 && \
  ./configure --without-tests --enable-shared && \
  make -j4 && \
  make install && \
  ldconfig /usr/local/lib && \
  cd .. && rm -f Python-3.12.13.tgz && rm -rf Python-3.12.13 && \
  ln -s /usr/local/bin/python3 /usr/local/bin/python && \
  ln -s /usr/local/bin/pip3 /usr/local/bin/pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  python -m pip install --upgrade pip; \
  pip install -r /tmp/requirements.txt;

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/2.1.0/flink-json-2.1.0.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-postgres-cdc/3.3.0/flink-connector-postgres-cdc-3.3.0.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.3.0/flink-sql-connector-postgres-cdc-3.3.0.jar;


RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;

WORKDIR /opt/flink
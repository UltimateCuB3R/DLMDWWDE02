FROM python:3.12-bookworm

ENV FLINK_VERSION=2.3.0 \
    FLINK_HOME=/opt/flink \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN curl -fsSL "https://downloads.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" -o /tmp/flink.tgz \
    && tar -xzf /tmp/flink.tgz -C /opt \
    && mv "/opt/flink-${FLINK_VERSION}" "${FLINK_HOME}" \
    && rm -f /tmp/flink.tgz

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /opt/flink/usrlib /opt/flink/logs \
    && ln -sf /usr/local/bin/python3.12 /usr/local/bin/python \
    && ln -sf /usr/local/bin/python3.12 /usr/local/bin/python3 \
    && ln -sf /usr/local/bin/pip3.12 /usr/local/bin/pip \
    && ln -sf /usr/local/bin/pip3.12 /usr/local/bin/pip3

COPY calc.py /opt/flink/usrlib/calc.py
COPY table_definition.xml /opt/flink/usrlib/table_definition.xml
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8081 6123 6124
WORKDIR ${FLINK_HOME}
ENTRYPOINT ["/docker-entrypoint.sh"]

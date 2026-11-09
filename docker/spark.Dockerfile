# Spark Python jobs on Ubuntu/Java 17.
FROM spark:3.5.5-scala2.12-java17-python3-ubuntu
USER root
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_BUNDLE_VERSION=1.12.262
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends \
      curl ca-certificates python3-venv python3-pip; \
    curl -fsSL --retry 4 "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o /opt/spark/jars/hadoop-aws.jar; \
    curl -fsSL --retry 4 "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" -o /opt/spark/jars/aws-java-sdk-bundle.jar; \
    rm -rf /var/lib/apt/lists/*
COPY pyproject.toml /opt/app/pyproject.toml
COPY dataflow/ /opt/app/dataflow/
WORKDIR /opt/app
RUN python3 -m venv /opt/twitter-venv && \
    /opt/twitter-venv/bin/python -m pip install --no-cache-dir --disable-pip-version-check /opt/app && \
    /opt/twitter-venv/bin/python -m pip check
ENV VIRTUAL_ENV=/opt/twitter-venv \
    PATH=/opt/twitter-venv/bin:$PATH \
    PYTHONPATH=/opt/app \
    PYSPARK_PYTHON=/opt/twitter-venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/twitter-venv/bin/python
USER 185
WORKDIR /opt/spark/work-dir

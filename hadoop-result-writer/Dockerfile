# syntax=docker/dockerfile:1
FROM python:3.13-slim-bookworm AS system
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl krb5-user libgssapi-krb5-2 openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ARG HADOOP_VERSION=3.4.2
ARG HADOOP_SHA512=79a383e156022d6690da359120b25db8146452265d92a4e890d9ea78c2078a01b661daf78163ee9b4acef7106b01fd5c8d1a55f7ad284f88b31ab3f402ae3acf
RUN curl -fSL --retry 3 "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz \
    && echo "${HADOOP_SHA512}  /tmp/hadoop.tar.gz" | sha512sum --check \
    && mkdir /opt/hadoop && tar -xzf /tmp/hadoop.tar.gz --strip-components=1 -C /opt/hadoop \
    && rm /tmp/hadoop.tar.gz \
    && ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /opt/java
ENV JAVA_HOME=/opt/java HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/etc/hadoop \
    HADOOP_LOG_DIR=/tmp/hadoop-logs KRB5_CONFIG=/etc/krb5.conf \
    PATH=/opt/venv/bin:/opt/hadoop/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

FROM system AS builder
RUN apt-get update && apt-get install -y --no-install-recommends gcc libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.11.23
WORKDIR /build
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --locked --no-dev --no-editable --extra pandas

FROM system AS runtime
RUN groupadd --gid 10001 writer && useradd --uid 10001 --gid writer --no-create-home writer
COPY --from=builder /opt/venv /opt/venv
USER 10001:10001
WORKDIR /tmp
ENTRYPOINT ["hadoop-result-writer"]
CMD ["--help"]

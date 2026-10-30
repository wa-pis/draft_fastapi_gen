FROM hadoop-result-writer:prototype
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-kdc krb5-admin-server openssl && rm -rf /var/lib/apt/lists/*
COPY . /integration
ENTRYPOINT ["python", "/integration/cluster.py"]

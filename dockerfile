FROM python:3.11-slim

# Use non-interactive frontend to avoid prompts during package installs
ENV DEBIAN_FRONTEND=noninteractive

# =========================
# Java 17 + outils requis
# =========================
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk-headless \
        procps \
        curl \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# =========================
# JAVA_HOME correct
# =========================
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# =========================
# Dossier de travail
# =========================
WORKDIR /app

# =========================
# Dépendances Python
# =========================
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    marimo \
    pandas \
    pyarrow \
    psycopg2-binary \
    pymongo \
    minio \
    requests

EXPOSE 8080 4040

CMD ["marimo", "edit", "--host", "0.0.0.0", "--port", "8080"]

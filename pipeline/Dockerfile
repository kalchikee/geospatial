FROM python:3.12-slim

# Install GDAL, PostgreSQL client (for raster2pgsql), and cron
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    postgresql-client \
    postgis \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
# Match GDAL version installed by apt
ENV GDAL_VERSION=3.6.2

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir GDAL==$(gdal-config --version) && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Install crontab
COPY crontab /etc/cron.d/pipeline-cron
RUN chmod 0644 /etc/cron.d/pipeline-cron && \
    crontab /etc/cron.d/pipeline-cron && \
    touch /var/log/pipeline.log

CMD ["cron", "-f"]

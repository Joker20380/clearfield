FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CLEARFIELD_PUBLIC_DIR=/opt/apps/clearfield \
    CLEARFIELD_PROJECT_DIR=/opt/apps/clearfield/clearfield \
    CLEARFIELD_VENV_DIR=/usr/local \
    CLEARFIELD_PYTHON=/usr/local/bin/python \
    CLEARFIELD_VENV_ACTIVATE=/opt/apps/clearfield/docker/activate \
    CLEARFIELD_FEED_DIR=/opt/apps/clearfield/generated-news

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates cron curl openssh-client rsync tzdata util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home --shell /bin/bash appuser

WORKDIR /opt/apps/clearfield/clearfield

COPY clearfield/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

COPY clearfield/ /opt/apps/clearfield/clearfield/
COPY docker/ /opt/apps/clearfield/docker/

RUN chmod +x /opt/apps/clearfield/docker/*.sh /opt/apps/clearfield/clearfield/bin/*.sh \
    && mkdir -p /opt/apps/clearfield/generated-news \
        /opt/apps/clearfield/clearfield/logs \
        /opt/apps/clearfield/clearfield/staticfiles \
        /opt/apps/clearfield/clearfield/var \
    && chown -R appuser:appuser /opt/apps/clearfield

EXPOSE 8105

ENTRYPOINT ["/opt/apps/clearfield/docker/entrypoint.sh"]
CMD ["gunicorn", "--workers", "2", "--timeout", "120", "--bind", "127.0.0.1:8105", "clearfield.wsgi:application"]

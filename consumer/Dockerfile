FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/cdc/consumer

COPY requirements.txt check-runtime.py ./

RUN python -m pip install --requirement requirements.txt \
    && python check-runtime.py \
    && groupadd --system --gid 10001 cdc \
    && useradd --system --uid 10001 --gid cdc --home-dir /nonexistent --shell /usr/sbin/nologin cdc \
    && install --directory --owner=cdc --group=cdc /data

COPY src ./src

ENV PYTHONPATH=/opt/cdc/consumer/src

USER cdc

CMD ["python", "-m", "cdc_consumer.runner"]

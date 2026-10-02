FROM python:3.14-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir uv && \
    uv pip install --system -r /tmp/requirements.txt && \
    pip uninstall -y pip

RUN groupadd --system default && \
    useradd --system --create-home --gid default --shell /usr/sbin/nologin default

COPY --chown=default:default --chmod=755 ./docker-entrypoint.sh /
COPY --chown=default:default ./server.py /

USER default

ENTRYPOINT ["/docker-entrypoint.sh"]

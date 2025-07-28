FROM python:3.12-slim-bullseye AS builder

    ENV UV_LINK_MODE=copy
    ENV UV_PROJECT_ENVIRONMENT=/usr/local/
    ENV UV_CACHE_DIR=/var/cache/uv

    COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
    COPY ./pyproject.toml pyproject.toml

    ENV UV_COMPILE_BYTECODE=1
    RUN --mount=type=cache,target=/var/cache \
        uv sync --no-install-project

    RUN --mount=type=cache,target=/root/.cache \
        apt-get update && apt-get -y install binutils upx && \
        python -c 'from sling.bin import *; download_binary(get_sling_version())' && \
        cd -- "$(dirname "$(find /root/.sling/ -type f -name sling | head -1)")" && \
        strip sling && upx sling


FROM python:3.12-slim-bullseye AS data_platform

    ENV DAGSTER_HOME=/opt/dagster/dagster_home

    COPY --from=builder /root/.sling/ /root/.sling/
    COPY --from=builder /usr/local/ /usr/local/
    COPY . /data_platform
    RUN mkdir -p $DAGSTER_HOME/ && \
        mv data_platform/dagster.yaml $DAGSTER_HOME/dagster.yaml

    ENV TARGET=prod
    WORKDIR /data_platform/

    EXPOSE 80

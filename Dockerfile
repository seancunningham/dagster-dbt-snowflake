FROM python:3.10-slim-bullseye AS builder

    ENV UV_LINK_MODE=copy
    ENV UV_PROJECT_ENVIRONMENT=/usr/local/
    ENV UV_CACHE_DIR=/var/cache/uv

    COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
    COPY ./pyproject.toml pyproject.toml

    # install project dependancies listed in pyproject.toml
    ENV UV_COMPILE_BYTECODE=1
    RUN --mount=type=cache,target=/var/cache \
        uv sync --no-install-project --no-dev

    # install sling binary for coresponding python module, and compress binary
    RUN --mount=type=cache,target=/root/.cache \
        python -c 'from sling.bin import *; download_binary(get_sling_version())' && \
        cd /root/.sling/bin/sling/ && cd $(ls -d */|head -n 1) && \
        apt-get update && apt-get -y install binutils upx && \
        strip sling && upx sling

    # install dbt-fusion (2.0.0-preview.7 for manifest compatability with dagster)
    RUN apt-get update && \
        apt-get -y install curl && \
        curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --version 2.0.0-preview.7 && \
        mv root/.local/bin/dbt usr/local/bin/dbt

FROM python:3.10-slim-bullseye AS data_platform

    ENV DAGSTER_HOME=/opt/dagster/dagster_home

    COPY --from=builder /root/.sling/ /root/.sling/
    COPY --from=builder /usr/local/ /usr/local/
    COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

    WORKDIR /data_platform/

    COPY pyproject.toml pyproject.toml
    COPY data_platform data_platform
    COPY dbt dbt
    
    COPY .env.prod .env
    
    ENV TARGET=prod

    ARG DESTINATION__PASSWORD
    ENV DESTINATION__PASSWORD=${DESTINATION__PASSWORD}

    RUN cd dbt && \
        dbt clean && \
        dbt deps && \
        dbt parse && \
        dbt compile
    
    EXPOSE 80

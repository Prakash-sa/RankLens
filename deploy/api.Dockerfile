FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --system --gid 10001 ranklens \
    && useradd --system --uid 10001 --gid ranklens --home /nonexistent --shell /usr/sbin/nologin ranklens

WORKDIR /opt/ranklens
COPY pyproject.toml README.md LICENSE alembic.ini ./
COPY constraints ./constraints
COPY python ./python
RUN python -m pip install --no-cache-dir -c constraints/enterprise.txt '.[enterprise]'

USER 10001:10001
EXPOSE 8080
CMD ["ranklens-enterprise-api", "--host", "0.0.0.0", "--port", "8080"]

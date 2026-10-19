# Single shared Dockerfile for all 8 placeholder services (workspace build).
# `python:3.11-slim` has NO curl — install it (HEALTHCHECK + smoke script).
FROM python:3.11-slim

ARG SERVICE_NAME
ENV SERVICE_NAME=${SERVICE_NAME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

COPY shared ./shared
COPY services ./services

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=5s --start-period=15s \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

ENTRYPOINT ["tini", "--"]
CMD ["uvicorn", "services.main:app", "--host", "0.0.0.0", "--port", "8080"]

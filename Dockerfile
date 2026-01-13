# syntax=docker/dockerfile:1.6
# A simple container for thera-py service.
# Runs service on port 80.
# Health checks service up every 5m.

FROM python:3.12-slim

WORKDIR /app

ARG IS_DEV_ENV="false"

ARG VERSION

ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_THERA_PY=$VERSION

RUN useradd --system --create-home --home-dir /home/appuser --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

COPY src/ ./src/
COPY pyproject.toml therapy-norm-update.sh ./

RUN pip install --upgrade pip setuptools setuptools_scm
RUN if [ $IS_DEV_ENV = "true" ]; then \
    pip install --no-cache-dir '.[etl]'; \
  else \
    pip install --no-cache-dir '.'; \
  fi

USER appuser

EXPOSE 80
HEALTHCHECK --interval=5m --timeout=3s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1/therapy', timeout=3).read()" || exit 1

CMD ["uvicorn", "therapy.main:app", "--port", "80", "--host", "0.0.0.0"]

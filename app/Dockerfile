# Reproducible build/test image for the Retail Genie App.
# The same image is built in CI, scanned, tested, tagged with the git SHA and pushed to the registry;
# Databricks Apps deploys the identical source tree (app.yaml + requirements.txt) via the bundle.
FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN useradd -m appuser && chown -R appuser /app
USER appuser
ENV DATABRICKS_APP_PORT=8000
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request as u;u.urlopen('http://localhost:8000/healthz')"
CMD ["python", "main.py"]

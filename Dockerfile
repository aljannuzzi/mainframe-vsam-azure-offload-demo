FROM python:3.11-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

COPY pyproject.toml ./
RUN python -c "import pathlib,tomllib; p=tomllib.loads(pathlib.Path('pyproject.toml').read_text()); pathlib.Path('/tmp/requirements.txt').write_text('\n'.join(p['project']['dependencies']))" \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --create-home --uid 10001 demo

COPY src/vsam_offload ./src/vsam_offload
COPY samples/copybooks ./samples/copybooks
USER demo
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "vsam_offload.guided_app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]

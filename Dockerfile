FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY app ./app
ENV PATH="/app/.venv/bin:$PATH"

# docker compose will override the cmd for the different services
CMD ["python", "-m", "app.producers.event_generator"]

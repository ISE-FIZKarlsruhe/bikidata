# syntax=docker/dockerfile:1
FROM python:3.12-slim

# uv from Astral: https://docs.astral.sh/uv/
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Install the bikidata dependencies from the lockfile.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# The bikidata source itself (virtual project: not installed into the venv).
ENV PYTHONPATH=/app/src

# Bind on all interfaces inside the container; override with `docker run -e`.
ENV HOST=0.0.0.0
ENV PORT=8000

EXPOSE 8000

# Same as `python -m bikidata serve`: run the FastAPI SPARQL server via uvicorn.
CMD ["python", "-m", "bikidata", "serve"]

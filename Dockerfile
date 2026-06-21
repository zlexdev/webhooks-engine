FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY libs/ libs/
COPY webhook_engine/ webhook_engine/

# Install both DB backends; pick one at runtime via WHE_BACKEND. The optional
# event-bus fan-out (the private `asyncbus` package) is intentionally NOT
# installed — the core delivery service runs without it.
RUN pip install --no-cache-dir -e ".[pg,mongo]"

EXPOSE 8080

CMD ["webhook-engine"]

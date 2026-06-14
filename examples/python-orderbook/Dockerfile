# Stdlib-only engine: no pip install, tiny image, fast cold start.
FROM python:3.12-slim
WORKDIR /app
COPY engine.py /app/engine.py
EXPOSE 8080
# Run unprivileged — the platform also drops caps / pins resources, but a
# good submission defends itself too.
USER 65534:65534
CMD ["python", "-u", "engine.py"]

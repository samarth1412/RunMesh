FROM python:3.13-slim
RUN groupadd --system --gid 10001 runmesh && useradd --system --uid 10001 --gid 10001 --home /app runmesh
WORKDIR /app
COPY sdk/python /sdk
RUN pip install --no-cache-dir /sdk
COPY examples/python_worker.py /app/worker.py
USER 10001:10001
CMD ["python", "worker.py"]

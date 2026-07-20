FROM python:3.13-slim
RUN groupadd --system runmesh && useradd --system --gid runmesh --home /app runmesh
WORKDIR /app
COPY sdk/python /sdk
RUN pip install --no-cache-dir /sdk
COPY examples/python_worker.py /app/worker.py
USER runmesh
CMD ["python", "worker.py"]

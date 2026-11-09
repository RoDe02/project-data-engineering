FROM python:3.11.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /opt/app
COPY pyproject.toml ./
COPY dataflow/ ./dataflow/
RUN pip install --no-cache-dir --disable-pip-version-check . && \
    addgroup --system --gid 10001 app && adduser --system --uid 10001 --ingroup app app
USER 10001:10001
ENTRYPOINT ["python", "-m"]
CMD ["dataflow.daily"]

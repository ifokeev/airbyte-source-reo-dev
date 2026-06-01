FROM airbyte/source-declarative-manifest:7.19.1

WORKDIR /airbyte/integration_code

COPY source_reo_dev/manifest.yaml ./source_declarative_manifest/manifest.yaml
COPY source_reo_dev/components.py ./components.py

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.2.2
LABEL io.airbyte.name=ifokeev/source-reo-dev

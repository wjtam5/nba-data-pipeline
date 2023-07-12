FROM apache/airflow:2.5.0

USER airflow
# Dependencies
COPY requirements.txt /
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# GCP SDK files and Variables
ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version
# Finish GCP SDK files and variables

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

# FROM apache/airflow:2.5.0

# ENV AIRFLOW_HOME=/opt/airflow

# USER root
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#     build-essential \
#     && apt-get autoremove -yqq --purge \
#     && apt-get -y install libpq-dev gcc \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# ## Start Python packages
# COPY requirements.txt .
# RUN pip install --upgrade pip
# RUN pip install --no-cache-dir -r requirements.txt
# ## Finish Python packages

# # Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

# 

# #WORKDIR $AIRFLOW_HOME

#
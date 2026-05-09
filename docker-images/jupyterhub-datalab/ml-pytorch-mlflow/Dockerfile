FROM quay.io/jupyter/pytorch-notebook:python-3.11

USER root


ARG ROOT_PATH=docker-images/jupyterhub-datalab/ml-pytorch-mlflow

ARG NB_USER=jovyan
ARG NB_UID=1000

ENV NB_USER=${NB_USER}
ENV NB_UID=${NB_UID}
ENV HOME=/home/${NB_USER}

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    vim \
    build-essential \
    graphviz \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

COPY ${ROOT_PATH}/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -m ipykernel install --user --name ml-python --display-name "Python ML"

ENV MLFLOW_TRACKING_URI=http://mlflow.mlops.svc.cluster.local:5000
ENV MLFLOW_S3_ENDPOINT_URL=http://minio.data-platform.svc.cluster.local:9000
ENV AWS_DEFAULT_REGION=us-east-1
ENV TOKENIZERS_PARALLELISM=false
ENV HF_HOME=/home/jovyan/.cache/huggingface
ENV TRANSFORMERS_CACHE=/home/jovyan/.cache/huggingface/transformers
ENV MPLCONFIGDIR=/home/jovyan/.cache/matplotlib



USER ${NB_USER}

WORKDIR ${HOME}

FROM saidsow/spark:3.5.8

USER root

ARG NB_USER=jovyan
ARG NB_UID=1000

ENV NB_USER=${NB_USER}
ENV NB_UID=${NB_UID}
ENV HOME=/home/${NB_USER}
ENV JUPYTER_RUNTIME_DIR=${HOME}/.local/share/jupyter/runtime
ENV JUPYTER_DATA_DIR=${HOME}/.local/share/jupyter

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
    jupyterlab==4.3.4 \
    jupyterhub \
    jupyterlab-git \
    jupyterlab-night \
    theme-darcula

RUN useradd -m -s /bin/bash -u ${NB_UID} ${NB_USER} \
    && mkdir -p ${HOME}/.local/share/jupyter/runtime ${HOME}/.jupyter \
    && chown -R ${NB_USER}:${NB_USER} ${HOME} /opt/spark

USER ${NB_USER}

WORKDIR ${HOME}
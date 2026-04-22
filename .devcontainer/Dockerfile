FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y \
    locales \
    curl \
    wget \
    git \
    vim \
    nano \
    unzip \
    openssh-client \
    python3 \
    python3-pip \
    python3-venv \
    sshpass \
    iputils-ping \
    default-jre-headless \
    dnsutils \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Install Kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/

# Install ArgoCD CLI
RUN curl -sSL -o /usr/local/bin/argocd \
    https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64 \
    && chmod +x /usr/local/bin/argocd

# Install mc (MinIO Client)
RUN curl -sSL -o /usr/local/bin/mc \
    https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x /usr/local/bin/mc

# Installer Trino CLI
RUN curl -L -o /usr/local/bin/trino \
    https://repo1.maven.org/maven2/io/trino/trino-cli/469/trino-cli-469-executable.jar \
    && chmod +x /usr/local/bin/trino

RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

RUN wget https://github.com/derailed/k9s/releases/latest/download/k9s_linux_amd64.deb \
    && apt-get update \
    && apt-get install -y ./k9s_linux_amd64.deb \
    && rm k9s_linux_amd64.deb

# Install Ansible
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --break-system-packages -r /tmp/requirements.txt

# Alias utiles
RUN echo "alias k=kubectl" >> /root/.bashrc \
    && echo "alias ll='ls -la'" >> /root/.bashrc \
    && echo "alias agc='argocd'" >> /root/.bashrc

ENV KUBECONFIG=/workspace/kubeconfig.yaml

WORKDIR /workspace
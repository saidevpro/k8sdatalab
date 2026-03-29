FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Outils de base
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
    dnsutils \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Installer kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/

# Installer Helm
RUN curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Installer Ansible
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --break-system-packages -r /tmp/requirements.txt

# Alias utiles
RUN echo "alias k=kubectl" >> /root/.bashrc \
    && echo "alias ll='ls -la'" >> /root/.bashrc

WORKDIR /workspace
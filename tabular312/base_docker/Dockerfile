# Base Image
FROM ubuntu:20.04

LABEL maintainer="Amazon AI"

# Define build-time variables
ARG PYTHON=python3
ARG PYTHON_PIP=python3-pip
ARG PIP=pip3
ARG PYTHON_VERSION=3.12.7

ENV DEBIAN_FRONTEND=noninteractive

# Install essential libraries and tools
RUN sed -i 's|http://archive.ubuntu.com/ubuntu/|http://ftp.kaist.ac.kr/ubuntu/|g' /etc/apt/sources.list \
        && apt-get update -o Acquire::ForceIPv4=true \
        && apt-get install -y --no-install-recommends \
        software-properties-common \
        build-essential \
        ca-certificates \
        curl \
        wget \
        git \
        libopencv-dev \
        openssh-client \
        openssh-server \
        vim \
        libffi-dev \
        libssl-dev \
        sqlite3 \
        libsqlite3-dev \
        libreadline-dev \
        libncursesw5-dev \
        tk-dev \
        libgdbm-dev \
        libc6-dev \
        libbz2-dev \
        zlib1g-dev && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python3 from source
RUN wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz && \
    tar -xvf Python-$PYTHON_VERSION.tgz && cd Python-$PYTHON_VERSION && \
    ./configure --enable-loadable-sqlite-extensions && \
    make && make install && \
    cd .. && rm -rf Python-$PYTHON_VERSION* && \
    ln -s /usr/local/bin/pip3 /usr/bin/pip && \
    ln -s /usr/local/bin/python3 /usr/bin/python

# Install fonts
RUN apt-get update && \
    apt-get install -y --no-install-recommends fontconfig && \
    wget https://hangeul.pstatic.net/hangeul_static/webfont/NanumGothic/NanumGothic.ttf && \
    mkdir -p /usr/share/fonts/nanum/ && \
    mv NanumGothic.ttf /usr/share/fonts/nanum/ && \
    fc-cache -fv && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# uv 설치
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# PIP 환경 변수를 uv pip으로 재정의
ENV PIP="pip"

# Upgrade pip and install Python packages
RUN ${PIP} --no-cache-dir install --upgrade pip && \
    ${PIP} install --upgrade setuptools

# Copy and install dependencies
WORKDIR /
COPY requirements.txt /requirements.txt

# PIP 환경 변수를 uv pip으로 재정의
ENV PIP="uv pip"
# RUN ${PIP} install --no-deps -r /requirements.txt && rm /requirements.txt
RUN ${PIP} install --no-deps --only-binary :all: -r /requirements.txt \
    || ${PIP} install --no-deps --prefer-binary -r /requirements.txt \
    || ${PIP} install --no-deps -r /requirements.txt || true \
    && rm /requirements.txt

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/usr/local/lib:/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib" \
    PYTHONIOENCODING=UTF-8 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8
FROM continuumio/miniconda3:25.1.1-2
ARG USERNAME
ARG GROUPNAME
ARG UID
ARG GID

RUN mkdir /app
WORKDIR /app

USER root

RUN apt-get update && apt-get install -y \
    sudo \
    wget \
    vim \
    unzip \
    libpq-dev

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install -y tzdata

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    cmake \
    sudo \
    build-essential

# gcloud インストールに必要な依存関係
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    graphviz \
    ca-certificates \
    && mkdir -p /etc/apt/keyrings \
    # Google Cloud SDK の署名鍵を追加
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
      | gpg --dearmor -o /etc/apt/keyrings/cloud.google.gpg \
    # リポジトリを登録
    && echo "deb [signed-by=/etc/apt/keyrings/cloud.google.gpg] \
      https://packages.cloud.google.com/apt cloud-sdk main" \
      | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    # SDK をインストール
    && apt-get update && apt-get install -y google-cloud-cli \
    # 不要ファイルを削除して軽量化
    && rm -rf /var/lib/apt/lists/*

#RUN apt-get install -y build-essential mecab libmecab-dev mecab-ipadic-utf8 fonts-ipafont-gothic file
RUN useradd -m $USERNAME
RUN gpasswd -a $USERNAME sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
ENV GRANT_SUDO=yes

EXPOSE 8888

RUN pip install --upgrade pip

RUN pip install poetry
ENV export PATH=/.poetry/bin:$PATH
COPY pyproject.toml ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-root

USER $USERNAME
ENV HOME=/app

#CMD ["jupyter", "lab", "--ip=0.0.0.0", "--NotebookApp.token=''", "--notebook-dir=/app", "--no-browser"]


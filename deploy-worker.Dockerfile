FROM prefecthq/prefect:3-latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    rsync \
    wget \
    curl \
    gnupg2 \
    git \
    ca-certificates \
    && wget https://github.com/apptainer/apptainer/releases/download/v1.3.4/apptainer_1.3.4_amd64.deb \
    && apt-get install -y ./apptainer_1.3.4_amd64.deb \
    && curl -sSL https://github.com/cli/cli/releases/download/v2.63.0/gh_2.63.0_linux_amd64.deb -o gh.deb \
    && apt-get install -y ./gh.deb \
    && rm -f apptainer_1.3.4_amd64.deb gh.deb \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    $$ pip install prefect-docker


# base
FROM ubuntu:20.04

# set the github runner version
ARG RUNNER_VERSION="2.294.0"
ENV RUNNER_VERSION=${RUNNER_VERSION}

# update the base packages and add a non-sudo user
RUN apt-get update -y && apt-get upgrade -y && useradd -m docker

# install python and the packages the your code depends on along with jq so we can parse JSON
# add additional packages as necessary
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl jq build-essential libssl-dev libffi-dev python3 python3-venv python3-dev python3-pip libpq-dev zip sudo unzip locales

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# cd into the user directory, download and unzip the github actions runner
RUN cd /home/docker && mkdir actions-runner && cd actions-runner \
    && curl -O -L https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz

RUN cd /home/docker/actions-runner && tar xzf ./actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz

RUN curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
RUN apt-get install -y nodejs

RUN npm install -g yarn

ENV DEBIAN_FRONTEND noninteractive

# install some additional dependencies
RUN chown -R docker ~docker && /home/docker/actions-runner/bin/installdependencies.sh

RUN apt-get update && apt-get install -y git
RUN pip install pipenv


# Request GITHUB_PAT
ARG GITHUB_PAT=DUMMY
ENV GITHUB_PAT=${GITHUB_PAT}

# Request REPO
ARG REPO=pulse-data
ENV REPO=${REPO}

# copy over the start.sh script
COPY entrypoint.sh entrypoint.sh

# make the script executable
RUN chmod +x entrypoint.sh

# since the config and run script for actions are not allowed to be run by root,
# set the user to "docker" so all subsequent commands are run as the docker user
USER docker

# set the entrypoint to the start.sh script
ENTRYPOINT ["./entrypoint.sh"]

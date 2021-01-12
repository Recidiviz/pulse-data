# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

# NOTE: This dockerfile is the source of the images we use to run our CI. If any
# changes are made to this, they need to be manually pushed by invoking:
#
# docker build .circleci/ -t recidiviz/circleci-build-img
# docker push recidiviz/circleci-build-img
#
# from the root of the project directory.

FROM cimg/python:3.7.9

# Install java
RUN sudo apt-get update && sudo apt-get install default-jdk -y

# Install Postgres
RUN sudo apt-get update && sudo apt-get install postgresql-10 -y && \
    sudo chown -R circleci:circleci /var/run/postgresql

# Install GCP Emulators
RUN sudo apt-get update && sudo apt-get install -y lsb-core && \
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - && \
    sudo apt-get update && sudo apt-get install google-cloud-sdk google-cloud-sdk-datastore-emulator google-cloud-sdk-pubsub-emulator -y

# Install Terraform
RUN curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add - && \
    sudo apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com $(lsb_release -cs) main" && \
    sudo apt install terraform

ENV PATH="/usr/lib/postgresql/10/bin:$PATH"

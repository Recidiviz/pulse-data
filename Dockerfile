FROM ubuntu:noble AS recidiviz-init
ENV DEBIAN_FRONTEND noninteractive
# NOTE: It is is extremely important that we do not delete this
# variable. One of our dependencies, dateparser, seems to require
# that TZ is defined (to be truly anything) in order to parse dates
# properly. If it is not defined, our date parsing will silently
# return None in a large set of circumstances which is of course,
# unideal.
ENV TZ America/New_York
# Add a package repo to get archived python versions.
RUN apt update -y && apt upgrade -y && \
    apt install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update -y && apt install -y \
    locales \
    git \
    libxml2-dev libxslt1-dev \
    default-jre \
    libpq-dev \
    libgeos-dev \
    build-essential \
    python3.11-dev \
    curl && rm -rf /var/lib/apt/lists/* && \
    apt-get clean
# Install dependencies for pymssql 
RUN apt-get update -y && \
    apt-get install -y freetds-dev gcc g++ unixodbc-dev
RUN locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LANG en_US.UTF-8
# Postgres pulls in tzdata which must have these set to stay noninteractive.
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
# Make stdout/stderr unbuffered. This prevents delay between output and cloud
# logging collection.
ENV PYTHONUNBUFFERED 1
RUN adduser recidiviz && mkdir /app && chown recidiviz /app/
USER recidiviz
RUN curl -s https://bootstrap.pypa.io/get-pip.py 2>&1 | python3.11
ENV PATH=/home/recidiviz/.local/bin:$PATH
RUN pip install uv --user
WORKDIR /app

FROM recidiviz-init AS recidiviz-dev
USER root
RUN apt update -y &&    \
    apt install -y      \
    apt-transport-https \
    iproute2            \
    npm                 \
    vim                 \
    wget
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg]"              \
    "http://packages.cloud.google.com/apt cloud-sdk main" |             \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list &&                  \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |             \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add /dev/stdin && \
    apt-get update -y && apt-get install google-cloud-cli -y
RUN wget -O /dev/stdout https://apt.releases.hashicorp.com/gpg             | \
    gpg --dearmor                                                          | \
    tee /usr/share/keyrings/hashicorp-archive-keyring.gpg &&                 \
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg]" \
    "https://apt.releases.hashicorp.com $(lsb_release -cs) main"      | \
    tee /etc/apt/sources.list.d/hashicorp.list &&                            \
    # Note: this verison number should be kept in sync with the ones in .github/workflows/ci.yml,
    # .devcontainer/devcontainer.json, recidiviz/tools/deploy/terraform/terraform.tf, and
    # recidiviz/tools/deploy/deploy_helpers.sh
    apt-get update -y && apt-get install terraform -y &&               \
    apt-mark hold terraform
# Install postgres to be used by tests that need to write to a database from multiple threads.
ARG PG_VERSION=13
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install postgresql-${PG_VERSION} -y
ENV PATH=/usr/lib/postgresql/${PG_VERSION}/bin/:$PATH
RUN apt-get update -y && apt-get upgrade -y
USER recidiviz
RUN npm install prettier
COPY pyproject.toml /app/
COPY uv.lock /app/
# --no-install-project avoids installing the project itself (source code isn't copied yet)
# --python specifies Python 3.11 since the system default may be different
RUN uv sync --all-extras --frozen --no-install-project --python python3.11
EXPOSE 8888

FROM node:20-alpine AS admin-panel-build
WORKDIR /usr/admin-panel
COPY ./frontends/admin-panel/package.json ./frontends/admin-panel/yarn.lock /usr/admin-panel/
COPY ./frontends/admin-panel/tsconfig.json ./frontends/admin-panel/.eslintrc.json /usr/admin-panel/
# Set a 5 minute timeout instead of the default 30s. For some reason, when building with the
# --platform argument, it takes longer to download packages from yarn.
RUN yarn config set network-timeout 300000
RUN yarn
COPY ./frontends/admin-panel/src /usr/admin-panel/src
COPY ./frontends/admin-panel/public /usr/admin-panel/public
RUN yarn build

FROM recidiviz-init AS recidiviz-app
# Add only the pyproject.toml/uv.lock first to ensure we cache `uv sync` when application code is updated but not the lock file
COPY --chown=recidiviz pyproject.toml /app/
COPY --chown=recidiviz uv.lock /app/
# --frozen ensures we use exactly what's in the lock file without updating it
# --no-install-project avoids installing the project itself (source code isn't copied yet)
# --python specifies Python 3.11 since the system default may be different
RUN uv sync --frozen --no-install-project --python python3.11
# Add the rest of the application code once all dependencies are installed
COPY --chown=recidiviz . /app
# Add the built Admin Panel frontend to the image
COPY --chown=recidiviz --from=admin-panel-build /usr/admin-panel/build /app/frontends/admin-panel/build
# Add the current commit SHA as an env variable
ARG CURRENT_GIT_SHA=""
ENV CURRENT_GIT_SHA=${CURRENT_GIT_SHA}
EXPOSE 8080

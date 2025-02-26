FROM ubuntu:jammy AS recidiviz-init
ENV DEBIAN_FRONTEND noninteractive
# NOTE: It is is extremely important that we do not delete this
# variable. One of our dependencies, dateparser, seems to require
# that TZ is defined (to be truly anything) in order to parse dates
# properly. If it is not defined, our date parsing will silently
# return None in a large set of circumstances which is of course,
# unideal.
ENV TZ America/New_York
# Add a package repo to get archived python versions.
RUN apt update -y && \
    apt install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa
RUN apt update -y && \
    apt install -y \
    locales \
    git \
    libxml2-dev libxslt1-dev \
    python3.9-dev python3.9-distutils python3-pip \
    default-jre \
    libpq-dev \
    curl
RUN locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LANG en_US.UTF-8
# Postgres pulls in tzdata which must have these set to stay noninteractive.
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
# Make stdout/stderr unbuffered. This prevents delay between output and cloud
# logging collection.
ENV PYTHONUNBUFFERED 1
RUN pip3 install pipenv
# In order to use this Dockerfile with Cloud Run, PIPENV_VENV_IN_PROJECT must be set.
# If not, Cloud Run will try to "helpfully" create a new virtualenv for us which will not match our
# expected set of dependencies.
# The main effect of this variable is to create the pipenv environment in the `.venv` folder in the
# root of the project.
ENV PIPENV_VENV_IN_PROJECT="1"
WORKDIR /app

FROM recidiviz-init AS recidiviz-dev
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
    apt-get update -y && apt-get install terraform=1.2.9 -y &&               \
    apt-mark hold terraform
# Install postgres to be used by tests that need to write to a database from multiple threads.
ARG PG_VERSION=13
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install postgresql-${PG_VERSION} -y
ENV PATH=/usr/lib/postgresql/${PG_VERSION}/bin/:$PATH
RUN apt-get update -y && apt-get upgrade -y
RUN npm install prettier
COPY Pipfile /app/
COPY Pipfile.lock /app/
RUN pipenv sync --dev --verbose
EXPOSE 8888

FROM node:14-alpine AS admin-panel-build
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
# Add only the Pipfiles first to ensure we cache `pipenv sync` when application code is updated but not the Pipfiles
COPY Pipfile /app/
COPY Pipfile.lock /app/
RUN pipenv sync --verbose
# Add the rest of the application code once all dependencies are installed
COPY . /app
# Add the built Admin Panel frontend to the image
COPY --from=admin-panel-build /usr/admin-panel/build /app/frontends/admin-panel/build
# Add the current commit SHA as an env variable
ARG CURRENT_GIT_SHA=""
ENV CURRENT_GIT_SHA=${CURRENT_GIT_SHA}
# Install security patches
RUN apt-get update && apt-get -y upgrade
EXPOSE 8080
CMD pipenv run gunicorn -c gunicorn.gthread.conf.py --log-file=- -b :8080 recidiviz.server:app
# This makes docker not report that our container is healthy until the flask workers are
# started and returning 200 on the `/health` endpoint. This is initially only used by
# our docker-test Github Action.
HEALTHCHECK --interval=5s --timeout=3s \
    CMD curl -f http://localhost:8080/health || exit 1
RUN rm -rf /var/lib/apt/lists/*; \
    apt-get clean;

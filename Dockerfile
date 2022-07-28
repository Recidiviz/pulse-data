FROM node:14-alpine as admin-panel-build

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

FROM node:14-alpine as case-triage-build

WORKDIR /usr/case-triage
COPY ./frontends/case-triage/package.json ./frontends/case-triage/yarn.lock /usr/case-triage/
COPY ./frontends/case-triage/tsconfig.json ./frontends/case-triage/.eslintrc.json /usr/case-triage/
COPY ./frontends/case-triage/craco.config.js /usr/case-triage/

RUN yarn config set network-timeout 300000
RUN yarn

COPY ./frontends/case-triage/src /usr/case-triage/src
COPY ./frontends/case-triage/public /usr/case-triage/public

RUN yarn build

FROM node:14-alpine as justice-counts-build

WORKDIR /usr/justice-counts/control-panel
COPY ./frontends/justice-counts/control-panel/package.json /usr/justice-counts/control-panel
COPY ./frontends/justice-counts/control-panel/yarn.lock /usr/justice-counts/control-panel
COPY ./frontends/justice-counts/control-panel/tsconfig.json /usr/justice-counts/control-panel

RUN yarn config set network-timeout 300000
RUN yarn

COPY ./frontends/justice-counts/control-panel/src /usr/justice-counts/control-panel/src
COPY ./frontends/justice-counts/control-panel/public /usr/justice-counts/control-panel/public

RUN yarn build

FROM ubuntu:focal

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

# If DEV_MODE="True", then install dependencies required for running tests
ARG DEV_MODE="False"

# Install postgres to be used by tests that need to write to a database from multiple threads.
RUN if [ "$DEV_MODE" = "True" ]; \
    then apt-get install wget && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install postgresql-13 -y; \
    fi
# Add all the postgres tools installed above to the path, so that we can use pg_ctl, etc. in tests.
# Uses variable substitution to set PATH_PREFIX to '/usr/lib/postgresql/13/bin/' in DEV_MODE and otherwise leave it
# blank. Docker doesn't support setting environment variables within conditions, so we can't do this above.
ENV PATH_PREFIX=${DEV_MODE:+/usr/lib/postgresql/13/bin/:}
# Then prepend our path with whatever is in PATH_PREFIX.
ENV PATH="$PATH_PREFIX$PATH"

# In order to use this Dockerfile with Cloud Run, PIPENV_VENV_IN_PROJECT must be set.
# If not, Cloud Run will try to "helpfully" create a new virtualenv for us which will not match our
# expected set of dependencies.
# The main effect of this variable is to create the pipenv environment in the `.venv` folder in the
# root of the project.
ENV PIPENV_VENV_IN_PROJECT="1"

# Add only the Pipfiles first to ensure we cache `pipenv sync` when application code is updated but not the Pipfiles
COPY Pipfile /app/
COPY Pipfile.lock /app/

WORKDIR /app

RUN if [ "$DEV_MODE" = "True" ]; \
    then pipenv sync --dev; \
    else pipenv sync; \
    fi

# Add the rest of the application code once all dependencies are installed
COPY . /app

# Add the built Admin Panel, Case Triage, and Justice Counts frontends to the image
COPY --from=admin-panel-build /usr/admin-panel/build /app/frontends/admin-panel/build
COPY --from=case-triage-build /usr/case-triage/build /app/frontends/case-triage/build
COPY --from=justice-counts-build /usr/justice-counts/control-panel/build /app/frontends/justice-counts/control-panel/build

# Add the current commit SHA as an env variable
ARG CURRENT_GIT_SHA=""
ENV CURRENT_GIT_SHA=${CURRENT_GIT_SHA}

EXPOSE 8080
CMD pipenv run gunicorn -c gunicorn.conf.py --log-file=- -b :8080 recidiviz.server:app

# This makes docker not report that our container is healthy until the flask workers are
# started and returning 200 on the `/health` endpoint. This is initially only used by
# our docker-test Github Action.
HEALTHCHECK --interval=5s --timeout=3s \
    CMD curl -f http://localhost:8080/health || exit 1

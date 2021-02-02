FROM node:14-alpine as admin-panel-build

WORKDIR /usr/admin-panel
COPY ./frontends/admin-panel/package.json ./frontends/admin-panel/yarn.lock /usr/admin-panel/
COPY ./frontends/admin-panel/tsconfig.json ./frontends/admin-panel/.eslintrc.json /usr/admin-panel/

RUN yarn

COPY ./frontends/admin-panel/src /usr/admin-panel/src
COPY ./frontends/admin-panel/public /usr/admin-panel/public

RUN yarn build

FROM node:14-alpine as case-triage-build

WORKDIR /usr/case-triage
COPY ./frontends/case-triage/package.json ./frontends/case-triage/yarn.lock /usr/case-triage/
COPY ./frontends/case-triage/tsconfig.json ./frontends/case-triage/.eslintrc.json /usr/case-triage/

RUN yarn

COPY ./frontends/case-triage/src /usr/case-triage/src
COPY ./frontends/case-triage/public /usr/case-triage/public

RUN yarn build

FROM ubuntu:bionic

RUN apt update -y && \
    apt install -y \
        locales \
        git \
        libxml2-dev libxslt1-dev \
        python3.7-dev python3-pip \
        default-jre \
        libpq-dev

RUN locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LANG en_US.UTF-8

ENV TZ America/New_York

# Postgres pulls in tzdata which must have these set to stay noninteractive.
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
ENV DEBIAN_FRONTEND=noninteractive

# Make stdout/stderr unbuffered. This prevents delay between output and cloud
# logging collection.
ENV PYTHONUNBUFFERED 1

RUN pip3 install pipenv

# If DEV_MODE="True", then install dependencies required for running tests
ARG DEV_MODE="False"

# Install the google cloud sdk to enable the gcp emulator (eg. fake datastore, fake pubsub)
# As described in: https://stackoverflow.com/questions/48250338/installing-gcloud-on-travis-ci
RUN if [ "$DEV_MODE" = "True" ]; \
    then apt-get update && apt install -y lsb-core && \
         export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
         echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
         curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
         apt update -y && apt-get install google-cloud-sdk -y && \
         apt install google-cloud-sdk-datastore-emulator -y && \
         apt install google-cloud-sdk-pubsub-emulator -y; \
    fi

# Install postgres to be used by tests that need to write to a database from multiple threads.
RUN if [ "$DEV_MODE" = "True" ]; \
    then apt-get update && apt install postgresql-10 -y; \
    fi
# Add all the postgres tools installed above to the path, so that we can use pg_ctl, etc. in tests.
# Uses variable substitution to set PATH_PREFIX to '/usr/lib/postgresql/10/bin/' in DEV_MODE and otherwise leave it
# blank. Docker doesn't support setting environment variables within conditions, so we can't do this above.
ENV PATH_PREFIX=${DEV_MODE:+/usr/lib/postgresql/10/bin/:}
# Then prepend our path with whatever is in PATH_PREFIX.
ENV PATH="$PATH_PREFIX$PATH"

# In order to use this Dockerfile with Cloud Run, PIPENV_VENV_IN_PROJECT must be set.
# If not, Cloud Run will try to "helpfully" create a new virtualenv for us which will not match our
# expected set of dependencies.
# The main effect of this variable is to create the pipenv environment in the `.venv` folder in the
# root of the project.
ENV PIPENV_VENV_IN_PROJECT="1"

# Add only the Pipfiles first to ensure we cache `pipenv sync` when application code is updated but not the Pipfiles
ADD Pipfile /app/
ADD Pipfile.lock /app/

WORKDIR /app

RUN if [ "$DEV_MODE" = "True" ]; \
    then pipenv sync --dev; \
    else pipenv sync; \
    fi

# Add the rest of the application code once all dependencies are installed
ADD . /app

# Add the built admin panel frontend to the image
COPY --from=admin-panel-build /usr/admin-panel/build /app/frontends/admin-panel/build
COPY --from=case-triage-build /usr/case-triage/build /app/frontends/case-triage/build

EXPOSE 8080
CMD pipenv run gunicorn -c gunicorn.conf.py --log-file=- -b :8080 recidiviz.server:app

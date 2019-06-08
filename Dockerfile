FROM ubuntu:latest

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

# Make stdout/stderr unbuffered. This prevents delay between output and cloud
# logging collection.
ENV PYTHONUNBUFFERED 1

RUN pip3 install pipenv

# If DEV_MODE="True", then install dependencies required for running tests
ARG DEV_MODE="False"

# Install the google cloud sdk to enable the gcp emulator (eg. fake datastore, fake pubsub)
# As described in: https://stackoverflow.com/questions/48250338/installing-gcloud-on-travis-ci
RUN if [ "$DEV_MODE" = "True" ]; \
    then apt install -y lsb-core && \
         export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
         echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
         curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
         apt update -y && apt-get install google-cloud-sdk -y && \
         apt install google-cloud-sdk-datastore-emulator -y && \
         apt install google-cloud-sdk-pubsub-emulator -y; \
    fi

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

EXPOSE 8080
CMD pipenv run gunicorn -c gunicorn.conf.py --log-file=- -b :8080 recidiviz.server:app

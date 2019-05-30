FROM ubuntu:latest

RUN apt update -y && \
    apt install -y \
        locales \
        git \
        python3.7-dev python3-pip \
        default-jre

RUN locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LANG en_US.UTF-8

ENV TZ America/New_York

# Make stdout/stderr unbuffered. This prevents delay between output and cloud
# logging collection.
ENV PYTHONUNBUFFERED 1

RUN pip3 install pipenv

## Add only the Pipfiles first to ensure we cache `pipenv sync` when application code is updated but not the Pipfiles
COPY recidiviz/read_pdf/Pipfile /app/
COPY recidiviz/read_pdf/Pipfile.lock /app/

WORKDIR /app

RUN pipenv sync

# Add the rest of the application code once all dependencies are installed
COPY ./recidiviz/read_pdf /app/recidiviz/read_pdf
COPY ./recidiviz/utils /app/recidiviz/utils

EXPOSE 8080
CMD pipenv run gunicorn -b :8080 recidiviz.read_pdf.main:app

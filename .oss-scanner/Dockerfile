# Build environment for Anthropic's OSS Scanner (https://github.com/anthropics/oss-scanner).
# The scanner builds this file with the pulse-data checkout as the build context, with network
# access, and then runs its audit with no network. Everything the tests need is installed here.
# The package list mirrors Dockerfile.recidiviz-base and the recidiviz-dev stage of Dockerfile.
FROM ubuntu:noble

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1
# dateparser returns None for many inputs when TZ is unset, so this must stay set.
ENV TZ=America/New_York

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
      locales git build-essential \
      python3.11 python3.11-dev python3.11-venv python3-pip \
      libxml2-dev libxslt1-dev libpq-dev libgeos-dev \
      default-jre freetds-dev unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8
ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime

# Tests that need a database start their own postgres with pg_ctl, so the server binaries must be
# on PATH. The version matches PG_VERSION in the repository's Dockerfile.
ARG PG_VERSION=18
RUN install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
         https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc]" \
         "http://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
         > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-${PG_VERSION} \
    && rm -rf /var/lib/apt/lists/*
ENV PATH=/usr/lib/postgresql/${PG_VERSION}/bin:$PATH

# uv is a static binary; the system pip only fetches it. Dependencies go into /src/.venv below.
RUN python3 -m pip install --break-system-packages "uv>=0.11.8"

# scanner contract: the checkout lives inside the image, at /src
# pg_ctl refuses to run as root, so the checkout and the tests run as an unprivileged user.
RUN useradd --create-home recidiviz
COPY --chown=recidiviz . /src
WORKDIR /src
USER recidiviz

# --frozen installs exactly what uv.lock pins. --all-extras adds the test and lint tools.
RUN uv sync --all-extras --frozen --python python3.11
ENV PATH=/src/.venv/bin:$PATH

# Run one cheap test package so a broken image shows up in the build log, without letting a
# failing test abort the build. recidiviz/tests/common has no dependency on packages that the
# public mirror leaves out.
RUN (uv run --no-sync pytest recidiviz/tests/common -q -p no:cacheprovider \
     || echo "WARNING: pytest reported failures in recidiviz/tests/common")

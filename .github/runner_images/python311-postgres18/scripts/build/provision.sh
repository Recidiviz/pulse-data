#!/bin/bash
echo "${HELPER_SCRIPTS}"

# Make helpers available to all users
sudo chmod 777 "${HELPER_SCRIPTS}"

# Ensure uv installs to the runner's home, not root
sudo -i -u runner HELPER_SCRIPTS="${HELPER_SCRIPTS}" bash <<'EOF'
set -e
source "${HELPER_SCRIPTS}/etc-environment.sh"

# Install compilation dependencies
sudo apt-get update
sudo apt-get install -y \
  libreadline-dev `# Needed for postgresql` \
  libbz2-dev `# Needed for python bz2 extension` \
  gdal-bin `# Needed for fiona pypi`  \
  libgdal-dev `# Needed for fiona pypi` \
  default-libmysqlclient-dev `# Needed for mysqlclient in Airflow pypi` \
  build-essential `# Needed for general c compilation` \
  pkg-config `# Needed for mysqlclient in Airflow pypi` \
  libssl-dev `# Needed by PG18 pgcrypto, which dropped its internal crypto fallback`

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export UV_BIN="$HOME/.local/bin"
prepend_etc_environment_path "$UV_BIN"
export PATH="$UV_BIN:$PATH"

# Install required Python versions
uv python install 3.11.6
uv python install 3.11.8

# Install Postgres
echo "Installing PostgreSQL 18..."
export POSTGRESQL_ROOT="${HOME}/postgresql/"
wget https://ftp.postgresql.org/pub/source/v18.3/postgresql-18.3.tar.gz
tar -xzf postgresql-18.3.tar.gz
cd postgresql-18.3

# PG18's pgcrypto requires OpenSSL (it dropped the internal crypto fallback).
# Without --with-ssl=openssl, pgcrypto.so links without -lcrypto and fails to
# dlopen at runtime with "undefined symbol: EVP_cast5_cbc".
./configure --prefix="${POSTGRESQL_ROOT}" --with-ssl=openssl
make
make install

# Install pgcrypto extension
pushd contrib/pgcrypto
make
make install
popd

# Set up Postgres path for Github Runner
prepend_etc_environment_path "${POSTGRESQL_ROOT}/bin"
set_etc_environment_variable "PGDATA" "${POSTGRESQL_ROOT}/data"

rm -rf postgresql-18.3
EOF

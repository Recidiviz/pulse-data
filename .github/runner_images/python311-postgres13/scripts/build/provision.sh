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
  pkg-config `# Needed for mysqlclient in Airflow pypi`

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
export UV_BIN="$HOME/.local/bin"
prepend_etc_environment_path "$UV_BIN"
export PATH="$UV_BIN:$PATH"

# Install required Python versions
uv python install 3.11.6
uv python install 3.11.8

# Install Postgres
echo "Installing PostgreSQL 13..."
export POSTGRESQL_ROOT="${HOME}/postgresql/"
wget https://ftp.postgresql.org/pub/source/v13.21/postgresql-13.21.tar.gz
tar -xzf postgresql-13.21.tar.gz
cd postgresql-13.21

./configure --prefix="${POSTGRESQL_ROOT}"
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

rm -rf postgresql-13.21
EOF

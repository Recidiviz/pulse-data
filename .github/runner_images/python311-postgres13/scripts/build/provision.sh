#!/bin/bash
echo "${HELPER_SCRIPTS}"

# Make helpers available to all users
sudo chmod 777 "${HELPER_SCRIPTS}"

# Ensure pyenv installs to the runner's home, not root
sudo -i -u runner HELPER_SCRIPTS="${HELPER_SCRIPTS}" bash <<'EOF'
set -e
source "${HELPER_SCRIPTS}/etc-environment.sh"

# Install pyenv and plugins
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
git clone https://github.com/pyenv/pyenv.git "$PYENV_ROOT"
git clone https://github.com/pyenv/pyenv-doctor.git "$PYENV_ROOT/plugins/pyenv-doctor"
git clone https://github.com/pyenv/pyenv-update.git "$PYENV_ROOT/plugins/pyenv-update"
git clone https://github.com/pyenv/pyenv-virtualenv.git "$PYENV_ROOT/plugins/pyenv-virtualenv"

# Set up shell init (for Github Runner)
prepend_etc_environment_path "${PYENV_ROOT}/bin:${PYENV_ROOT}/shims"

# Install data platform Python version
pyenv install 3.11.6
# Install Airflow Python version
pyenv install 3.11.8
pyenv global 3.11.6
pip install --upgrade pip
pip install pipenv

# Install compilation dependencies
sudo apt-get update
sudo apt-get install -y \
  libreadline-dev `# Needed for postgresql` \
  gdal-bin `# Needed for fiona pypi`  \
  libgdal-dev `# Needed for fiona pypi` \
  default-libmysqlclient-dev `# Needed for mysqlclient in Airflow pypi` \
  build-essential `# Needed for general c compilation` \
  pkg-config `# Needed for mysqlclient in Airflow pypi`

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

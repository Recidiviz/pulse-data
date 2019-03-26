source travis/base.sh

# Setup git
git config user.email "helperbot@recidiviz.com" 2>&1 | ind
git config user.name "Helper Bot" 2>&1 | ind
echo "https://helperbot-recidiviz:${GH_TOKEN}@github.com" > .git/credentials 2>&1 | ind

# TODO(#1361): Use pre-built copybara image when it's available.
# Build the copybara image
docker build github.com/google/copybara.git -t copybara:latest 2>&1 | ind

# Run copybara
#
# Note: Copybara returns exit code 4 if the command was a no-op. This will
# happen if the change only modified files that are excluded from the public
# mirror. In this case we still want the script to pass, so we capture the exit
# code and only exit if it is not 0 or 4.
set +e
docker run -e COPYBARA_CONFIG='mirror/copy.bara.sky' \
           -e COPYBARA_WORKFLOW='exportSourceToPublic' \
           -v "$(pwd)/.git/config":/root/.gitconfig \
           -v "$(pwd)/.git/credentials":/root/.git-credentials \
           -v "$(pwd)":/usr/src/app \
           -it copybara copybara 2>&1 | ind
code=$? 
if [ "$code" -ne 0 -a "$code" -ne 4 ];
then
   exit $code
fi;
set -e

# Cleanup git
git config --unset user.email 2>&1 | ind
git config --unset user.name 2>&1 | ind
rm .git/credentials 2>&1 | ind

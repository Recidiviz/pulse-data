source travis/base.sh

# Setup git
git config user.email "helperbot@recidiviz.com" 2>&1 | ind
git config user.name "Helper Bot" 2>&1 | ind
echo "https://helperbot-recidiviz:${GH_TOKEN}@github.com" > .git/credentials 2>&1 | ind

# TODO(#1361): Use pre-built copybara image when it's available.
# Build the copybara image
docker build github.com/google/copybara.git -t copybara:latest 2>&1 | ind

# Run copybara
docker run -e COPYBARA_CONFIG='mirror/copy.bara.sky' \
           -e COPYBARA_WORKFLOW='exportSourceToPublic' \
           -v "$(pwd)/.git/config":/root/.gitconfig \
           -v "$(pwd)/.git/credentials":/root/.git-credentials \
           -v "$(pwd)":/usr/src/app \
           -it copybara copybara 2>&1 | ind

# Cleanup git
git config --unset user.email 2>&1 | ind
git config --unset user.name 2>&1 | ind
rm .git/credentials 2>&1 | ind

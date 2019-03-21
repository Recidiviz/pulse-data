echo "setup"
git config user.email "helperbot@recidiviz.com"
git config user.name "Helper Bot"
echo "https://helperbot-recidiviz:${GH_TOKEN}@github.com" > .git/credentials

# TODO(#1361): Use pre-built copybara image when it's available.
echo "build"
docker build github.com/google/copybara.git -t copybara:latest

echo "run"
docker run -e COPYBARA_CONFIG='mirror/copy.bara.sky' \
           -e COPYBARA_WORKFLOW='exportSourceToPublic' \
           -v "$(pwd)/.git/config":/root/.gitconfig \
           -v "$(pwd)/.git/credentials":/root/.git-credentials \
           -v "$(pwd)":/usr/src/app \
           -it copybara copybara

echo "cleanup"
git config --unset user.email
git config --unset user.name
rm .git/credentials

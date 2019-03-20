# Prepends '> ' to each line of input.
# Append `2>&1 | ind` to all commands so that all output goes to stdout and gets
# run through this function. 
BLUE=$(tput setaf 4)
RESET=$(tput sgr0)
function ind {
    sed "s/^/$BLUE>$RESET /"
}

# Use `set -v` so that the commands are printed to Travis. We should never use
# `set -x` as that expands variables before printing the commands, and some of
# the variables are secrets.
set -v

# Create encryption keys and authenticate with GAE
openssl aes-256-cbc -K $encrypted_f7b0b16e1068_key -iv $encrypted_f7b0b16e1068_iv -in client-secret-staging.json.enc -out client-secret-staging.json -d 2>&1 | ind
gcloud -q auth activate-service-account recidiviz-staging@appspot.gserviceaccount.com --key-file client-secret-staging.json 2>&1 | ind

# Deploy cron.yaml
gcloud -q app deploy cron.yaml --project=recidiviz-staging 2>&1 | ind

# Generate and deploy queue.yaml
docker exec -it recidiviz pipenv run python -m recidiviz.tools.build_queue_config --environment all 2>&1 | ind
docker cp recidiviz:/app/queue.yaml . 2>&1 | ind
gcloud -q app deploy queue.yaml --project=recidiviz-staging 2>&1 | ind

# App engine doesn't allow '.' in the version name
VERSION=$(echo $TRAVIS_TAG | tr '.' '-') 2>&1 | ind

# Authorize docker to acess GCR
# use instead of 'auth configure-docker' as travis has an old gcloud
gcloud -q docker --authorize-only 2>&1 | ind

# Push the docker image to GCR
IMAGE_URL=us.gcr.io/recidiviz-staging/appengine/default.$VERSION:latest 2>&1 | ind
docker tag recidiviz-image $IMAGE_URL 2>&1 | ind
docker push $IMAGE_URL 2>&1 | ind

# Deploy application
gcloud -q app deploy staging.yaml --project recidiviz-staging --version $VERSION --image-url $IMAGE_URL 2>&1 | ind

# Turn off verbose
set +v

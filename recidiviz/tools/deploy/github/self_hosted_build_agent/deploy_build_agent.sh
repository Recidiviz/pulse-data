#!/bin/bash
deploy_build_agent () {
  # Get the latest version of the Github Build Agent
  VERSION=$(curl --request GET --url https://api.github.com/repos/actions/runner/releases/latest | jq .tag_name --raw-output | cut -c 2- | xargs)

  # Pull a Github PAT for Helper Bot
  GITHUB_DEPLOY_BOT_TOKEN=$(get_secret recidiviz-123 github_deploy_script_pat))

  IMAGE_VERSION_NAME=gh-agent-$1:$VERSION
  # Build the image locally
  docker image build -t $IMAGE_VERSION_NAME --build-arg RUNNER_VERSION=$VERSION --build-arg GITHUB_PAT=$GITHUB_DEPLOY_BOT_TOKEN  --build-arg REPO=$1 .

  # Tag the image
  docker tag $IMAGE_VERSION_NAME us.gcr.io/recidiviz-devops/$IMAGE_VERSION_NAME

  # Push the image up to `recidiviz-devops`
  docker push us.gcr.io/recidiviz-devops/$IMAGE_VERSION_NAME

  # Deploy the latest version of Github Self-Hosted Build Agent
  gcloud app deploy $1.yaml --project=recidiviz-devops --image-url=us.gcr.io/recidiviz-devops/$IMAGE_VERSION_NAME --quiet
}

# Deploy pulse-data build agent
deploy_build_agent "pulse-data"

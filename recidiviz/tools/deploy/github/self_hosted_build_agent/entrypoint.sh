#!/bin/bash
REG_TOKEN=$(curl --request POST --url https://api.github.com/repos/Recidiviz/$REPO/actions/runners/registration-token --header 'Accept: application/vnd.github+json' --header "Authorization: token ${GITHUB_PAT}" | jq .token --raw-output)

cd /home/docker/actions-runner

./config.sh --url https://github.com/Recidiviz/$REPO --token $REG_TOKEN --disableupdate

cleanup() {
    echo "Removing runner..."
    REG_TOKEN=$(curl --request POST --url https://api.github.com/repos/Recidiviz/$REPO/actions/runners/registration-token --header 'Accept: application/vnd.github+json' --header "Authorization: token ${GITHUB_PAT}" | jq .token --raw-output)
    ./config.sh remove --unattended --token $REG_TOKEN
}

trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

./run.sh & wait $!

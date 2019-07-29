#!/usr/bin/env bash

deploy_name=$1
version_tag=$(echo $2 | tr '.' '-')

if [ x"$deploy_name" == x -o x"$version_tag" == x ]; then
    echo "usage: $0 <deploy_name> <version_tag>"
    exit 1
fi

function run_cmd {
    cmd="$1"
    echo "Running \`$cmd\`"
    $cmd

    ret_code=$?
    if [ $ret_code -ne 0 ]; then
        echo "Failed with exit code $ret_code"
        exit $ret_code
    fi
}

echo "Building docker image"
run_cmd "docker build -t recidiviz-image ."

echo "Tagging release"
run_cmd "docker tag recidiviz-image us.gcr.io/recidiviz-staging/appengine/$deploy_name"

echo "Pushing image"
run_cmd "docker push us.gcr.io/recidiviz-staging/appengine/$deploy_name"

echo "Running deploy"
run_cmd "gcloud -q app deploy --no-promote staging.yaml
       --project recidiviz-staging
       --version $version_tag-$deploy_name
       --image-url us.gcr.io/recidiviz-staging/appengine/$deploy_name
       --verbosity=debug"

echo "App deployed (but not promoted) to \`$version_tag-$deploy_name\`.recidiviz-staging.appspot.com"

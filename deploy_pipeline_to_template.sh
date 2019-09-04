#!/usr/bin/env bash

project=$1
bucket=$2
pipeline=$3
job_name=$4
input=$5
output=$6

if [ x"$project" == x -o x"$bucket" == x -o x"$pipeline" == x -o x"$job_name" == x -o x"$input" == x -o x"$output" == x ]; then
    echo "usage: $0 <project> <bucket> <pipeline> <job_name> <input> <output>"
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

echo "Deploying $pipeline pipeline to template"
run_cmd "python -m run_calculation_pipelines
    --pipeline $pipeline
    --job_name $job_name
    --runner DataflowRunner
    --project $project
    --setup_file ./setup.py
    --staging_location gs://$bucket/staging
    --temp_location gs://$bucket/temp
    --template_location gs://$bucket/templates/$job_name
    --input $input
    --output $output
    --methodology=BOTH --include-race=True --include_age=True --include_stay_length=True --include_ethnicity=True --include_gender=True --include_release_facility=True"


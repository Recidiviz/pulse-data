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

function check_not_empty {
    # Check if variable is empty and exit if it is
    [ -z "$1" ] && echo "Variable is unset" && exit 1
}

function deploy_pipelines {
  cmd=$1
  project=$2
  bucket=$3
  file=$4

  pipeline_count="$(cat $file | pipenv run yq -r .pipeline_count)"
  check_not_empty pipeline_count

  END=$pipeline_count
  ((END = END - 1))
  i=0
  # Deploy each pipeline in the file to a template
  while [[ $i -le $END ]]
  do
      pipeline="$(cat $file | pipenv run yq -r .pipelines[$i].pipeline)"
      check_not_empty pipeline
      job_name="$(cat $file | pipenv run yq -r .pipelines[$i].job_name)"
      check_not_empty job_name
      input="$(cat $file | pipenv run yq -r .pipelines[$i].input)"
      check_not_empty input
      output="$(cat $file | pipenv run yq -r .pipelines[$i].output)"
      check_not_empty output

      run_cmd "$cmd $project $bucket $pipeline $job_name $input $output"
      ((i = i + 1))
  done
}

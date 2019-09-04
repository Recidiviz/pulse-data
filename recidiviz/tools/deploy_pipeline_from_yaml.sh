source ./run_commands.sh

function deploy_pipelines {
  cmd=$1
  project=$2
  bucket=$3
  file=$4

  pipeline_count="$(cat $file | yq -r .pipeline_count)"
  END=$pipeline_count
  ((END = END - 1))
  i=0
  # Deploy each pipeline in the file to a template
  while [[ $i -le $END ]]
  do
      pipeline="$(cat $file | yq -r .pipelines[$i].pipeline)"
      job_name="$(cat $file | yq -r .pipelines[$i].job_name)"
      input="$(cat $file | yq -r .pipelines[$i].input)"
      output="$(cat $file | yq -r .pipelines[$i].output)"
      run_cmd "$cmd $project $bucket $pipeline $job_name $input $output"
      ((i = i + 1))
  done
}

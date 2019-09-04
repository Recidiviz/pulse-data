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

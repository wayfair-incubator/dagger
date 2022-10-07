prefix_cmd=""

# Prefix service startup with wait-for-it script if running in local (to wait for local Kafka to be up and ready)
if [[ "$FAUST_CONFIG" == "dagger.settings.local.LocalConfig" ]]
then
    prefix_cmd="wait-for-it -t 120 kf-fos_py:29092 -- "
fi

# For vscode devcontainer runtime scope. Prevents service from starting and instead hangs the container so that vscode can step in and attach to it
if [[ "$SERVICE_SCOPE" == "DEV_CONTAINER" ]]
then
    sleep infinity
# Run the specified service
elif [[ "$SERVICE" == "dagger_test_app" ]]
then
    ${prefix_cmd} python3 -m integration_tests.test_app worker -l info
else
    echo "Invalid service"
fi

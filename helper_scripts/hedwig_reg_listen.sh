#!/usr/bin/env bash

set -e -o pipefail

if [ ! -n "$PREFECT_API_KEY" ]; then
	printf "PREFECT_API_KEY environment var must be set.\n"
	printf "For example: export PREFECT_API_KEY =<token here>\n"
	printf "(Exiting on 1.)\n"
	exit 1
fi

SELF=`basename "$0"`
# HEDWIG_ENV=$1
if [[ ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
	printf "\nScript must be export HEDWIG_ENV to work.\n"
	printf "For example: export HEDWIG_ENV=dev\n"
	printf "(Exiting on 1.)\n"
	exit 1
fi


export PREFECT_API_URL="https://prefect2.hedwig-workflow-api.niaid$HEDWIG_ENV.net/api"
# this also defines where to register flows.
if [[ $HEDWIG_ENV == "dev" ]]; then
	HEDWIG_HOME="/gs1/home/hedwig_dev"
elif [[ $HEDWIG_ENV == "qa" ]]; then
	HEDWIG_HOME="/gs1/home/hedwig_qa"
elif [[ $HEDWIG_ENV == "prod" ]]; then
	HEDWIG_HOME="/gs1/home/hedwig_prod"
fi

# ensure the virtual env exists before we go any further.
if [[ ! -d ~/$HEDWIG_ENV ]]; then
	printf "$HEDWIG_ENV virtual env directory (~/$HEDWIG_ENV) does not exist. (Exiting on 1.)"
	exit 1
fi
source $HEDWIG_HOME/$HEDWIG_ENV/bin/activate

ACTION=$1
if [[ ! ( $ACTION == "listen" || $ACTION == "register" || $ACTION == "setup" || $ACTION == "info" ) ]]; then
	printf "Script must be called with ARG1 as either listen, register, setup, or info."
	printf "Eg ./$SELF listen\nExiting on 1."
	exit 1
fi

# start worker specific to THAT venv
WORKPOOL=workpool

# Expecting the repo to be accessible directly from HOME
cd ~/image_portal_workflows

if [[ $ACTION == "listen" ]]; then
	printf "\nStarting $HEDWIG_ENV Worker\n"
	sh -c "prefect worker start --pool $WORKPOOL"
elif [[ $ACTION == "register" ]]; then
	printf "\nRegister $HEDWIG_ENV Agent\n"
  # all the configs are in ProjectRoot/prefect.yaml
	sh -c "prefect deploy --all"
elif [[ $ACTION == "setup" ]]; then
	printf "\nSetting up Prefect $WORKPOOL\n"
	sh -c "prefect work-pool create $WORKPOOL --type process"
elif [[ $ACTION == "info" ]]; then
	printf "\nFetching Prefect $WORKPOOL Deployments\n"
  curl -X 'POST' "$PREFECT_API_URL/deployments/filter" -H 'accept: application/json' -H "Authorization: Bearer $PREFECT_API_KEY" -H 'Content-Type: application/json' -d '{}' | jq -r '.[] | "\(.id) \t\t \(.entrypoint) \t\t \(.description)"'
fi

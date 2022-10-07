#!/usr/bin/env bash

set -e -o pipefail

NIAID_PREFECT_SERVER="http://44.192.88.156:4200"

SELF=`basename "$0"`
HEDWIG_ENV=$1
if [[ ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
	printf "\nScript must be called with ARG1 either qa, dev, or prod.\n"
	printf "Eg ./$SELF dev (Exiting on 1.)\n"
	exit 1
fi

export $HEDWIG_ENV

ACTION=$2
if [[ ! ( $ACTION == "listen" || $ACTION == "register" ) ]]; then
	printf "Script must be called with ARG2 as either listen, or register."
	printf "Eg ./$SELF dev listen\nExiting on 1."
	exit 1
fi

# Work out location of python venv,
# ensure dir exists,
# start agent specific to THAT venv
# see: https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VENV=$SCRIPT_DIR/../../../$HEDWIG_ENV
WFLOWS=$SCRIPT_DIR/../em_workflows
PREFECT=$VENV/bin/prefect
PYTHON=$VENV/bin/python3

# ensure the virtual env exists before we go any further.
if [[ ! -d $VENV ]]; then
	printf "$HEDWIG_ENV virtual env directory ($VENV) does not exist. (Exiting on 1.)"
	exit 1
fi

if [[ $ACTION == "listen" ]]; then
	printf "\nUsing venv $VENV\nStarting $HEDWIG_ENV Agent\n"
	$PYTHON $PREFECT backend server
	$PYTHON $PREFECT agent local start --label $HEDWIG_ENV --api $NIAID_PREFECT_SERVER
elif [[ $ACTION == "register" ]]; then
	printf "\nUsing venv $VENV\nRegister $HEDWIG_ENV Agent\n"
	printf "$PYTHON $PREFECT register --project "Spaces_$HEDWIG_ENV" --watch --path $WFLOWS/brt/"
	printf "\n\n"
	printf "$PYTHON $PREFECT register --project "Spaces_$HEDWIG_ENV" --watch --path $WFLOWS/sem_tomo/"
	printf "\n\n"
	printf "$PYTHON $PREFECT register --project "Spaces_$HEDWIG_ENV" --watch --path $WFLOWS/dm_conversion/"
fi

#!/usr/bin/env bash

set -e -o pipefail


SELF=`basename "$0"`
HEDWIG_ENV=$1
if [[ ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
	printf "\nScript must be called with ARG1 either qa, dev, or prod.\n"
	printf "Eg ./$SELF dev (Exiting on 1.)\n"
	exit 1
fi


# this also defines where to register flows.
if [[ $HEDWIG_ENV == "dev" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaiddev.net"
elif [[ $HEDWIG_ENV == "qa" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaidqa.net"
elif [[ $HEDWIG_ENV == "prod" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaidprod.net"
fi


export HEDWIG_ENV=$HEDWIG_ENV
export IMOD_DIR=/opt/rml/imod


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
export VENV
_OLD_VIRTUAL_PATH="$PATH"
PATH="$VENV/bin:$PATH"
export PATH
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
	$PYTHON $PREFECT agent local start --label $HEDWIG_ENV --api $PREFECT__SERVER__HOST:4200
elif [[ $ACTION == "register" ]]; then
	printf "\nUsing venv $VENV\nRegister $HEDWIG_ENV Agent\n"
	$PYTHON $PREFECT register --project Spaces_$HEDWIG_ENV  --path $WFLOWS/brt/
	$PYTHON $PREFECT register --project Spaces_$HEDWIG_ENV  --path $WFLOWS/sem_tomo/
	$PYTHON $PREFECT register --project Spaces_$HEDWIG_ENV  --path $WFLOWS/dm_conversion/
fi

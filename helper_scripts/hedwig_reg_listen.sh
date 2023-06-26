#!/usr/bin/env bash

set -e -o pipefail


SELF=`basename "$0"`
# HEDWIG_ENV=$1
if [[ ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
	printf "\nScript must be export HEDWIG_ENV to work.\n"
	printf "For example: export HEDWIG_ENV=dev\n"
	printf "(Exiting on 1.)\n"
	exit 1
fi


# this also defines where to register flows.
if [[ $HEDWIG_ENV == "dev" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaiddev.net"
	HEDWIG_HOME="/gs1/home/hedwig_dev"
elif [[ $HEDWIG_ENV == "qa" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaidqa.net"
	HEDWIG_HOME="/gs1/home/hedwig_qa"
elif [[ $HEDWIG_ENV == "prod" ]]; then
	export PREFECT__SERVER__HOST="https://prefect1.hedwig-workflow-api.niaidprod.net"
	HEDWIG_HOME="/gs1/home/hedwig_prod"
fi

# ensure the virtual env exists before we go any further.
if [[ ! -d ~/$HEDWIG_ENV ]]; then
	printf "$HEDWIG_ENV virtual env directory (~/$HEDWIG_ENV) does not exist. (Exiting on 1.)"
	exit 1
fi
source $HEDWIG_HOME/$HEDWIG_ENV/bin/activate

export IMOD_DIR=/opt/rml/imod


ACTION=$1
if [[ ! ( $ACTION == "listen" || $ACTION == "register" || $ACTION == "create_proj" ) ]]; then
	printf "Script must be called with ARG1 as either listen, or register."
	printf "Eg ./$SELF listen\nExiting on 1."
	exit 1
fi

# start agent specific to THAT venv
WFLOWS="$HEDWIG_HOME/image_portal_workflows/em_workflows"
PROJ="Spaces_$HEDWIG_ENV"
if [[ $ACTION == "listen" ]]; then
	printf "\nStarting $HEDWIG_ENV Agent\n"
	prefect backend server
	prefect agent local start --label $HEDWIG_ENV --api $PREFECT__SERVER__HOST:4200
elif [[ $ACTION == "register" ]]; then
	printf "\nRegister $HEDWIG_ENV Agent\n"
	prefect register --project $PROJ  --path $WFLOWS/brt/
	prefect register --project $PROJ  --path $WFLOWS/sem_tomo/
	prefect register --project $PROJ  --path $WFLOWS/dm_conversion/
	if [[ ! -d $WFLOWS/lrg_2d_rgb/ ]]; then
		$PYTHON $PREFECT register --project Spaces_$HEDWIG_ENV  --path $WFLOWS/lrg_2d_rgb/
	fi
elif [[ $ACTION == "create_proj" ]]; then
	prefect create project $PROJ
fi

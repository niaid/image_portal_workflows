#!/bin/bash

# creates a virtual env to load the dev workflow source into.

set -xe -o pipefail

HEDWIG_ENV=$1
echo "env is $HEDWIG_ENV"
if [[ ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
	echo Environment MUST be set to either "prod", "qa" or "dev".
	echo Eg export HEDWIG_ENV=dev 
	exit 1
fi

VENV_LOC="$HOME/code/hedwig/$HEDWIG_ENV"
REPO_LOC=$VENV_LOC/image_portal_workflows

if [[ -d $VENV_LOC ]]; then
	printf "Virtual env $VENV_LOC already exists. Exiting."
	exit 1
fi
# create a venv
python3 -m venv $VENV_LOC

# fire her up
source $VENV_LOC/bin/activate

# probably wants this...
python3 -m pip install --upgrade pip

# grab Brad's work
python -m pip install git+https://github.com/niaid/tomojs-pytools@v1.3.6


# grab or update this repo
if [[ ! -d "$REPO_LOC" ]]; then
	git clone git@github.com:niaid/image_portal_workflows.git $REPO_LOC && \
	cd $REPO_LOC && pip3 install -e . -r $REPO_LOC/requirements.txt
else
	cd $REPO_LOC && git pull origin && cd -
fi


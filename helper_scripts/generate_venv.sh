#!/bin/bash
THIS IS NOT CURRENTLY USED - setup is documented in hpc.rst

# creates a virtual env to load the dev workflow source into.

#set -e -o pipefail
#if [[ -z "${HEDWIG_ENV}" || ! ( $HEDWIG_ENV == "dev" || $HEDWIG_ENV == "qa" || $HEDWIG_ENV == "prod" ) ]]; then
#	echo Environment MUST be set to either "prod", "qa" or "dev".
#	echo Eg export HEDWIG_ENV=dev
#	echo Unable to proceed, exiting on 1.
#	exit 1
#fi
#echo "HEDWIG_ENV env is $HEDWIG_ENV"
#
#VENV_LOC="$HOME/$HEDWIG_ENV"
#REPO_LOC="$HOME/image_portal_workflows"
#
#if [[ -d $VENV_LOC ]]; then
#	printf "Virtual env $VENV_LOC already exists. Updating."
#fi
## create a venv
#python3 -m venv $VENV_LOC
#
## fire her up
#source $VENV_LOC/bin/activate
#
## probably wants this...
#python3 -m pip install --upgrade pip
#
## grab or update this repo
#if [[ ! -d "$REPO_LOC" ]]; then
#	git clone git@github.com:niaid/image_portal_workflows.git $REPO_LOC && \
#	cd $REPO_LOC && pip3 install -e . -r $REPO_LOC/requirements.txt
#else
#	cd $REPO_LOC && git pull origin && cd -
#fi

#!/bin/bash

echo
if [[ ! $(prefect config view | grep PREFECT_API_KEY) ]]; then
  echo "PREFECT_API_KEY is not set; Set using 'prefect config set PREFECT_API_KEY=*****'";
  exit 1;
fi

base_url="https://prefect2.hedwig-workflow-api.niaiddev.net";
api_url="$base_url/api";
echo
if [[ ! $(curl $api_url/health | grep "true") ]]; then
  echo
  echo "Make sure you are connected to NIAID VPN";
  exit 1;
fi

branch=`git branch --show-current`
echo "Sending pytest runner for branch=$branch";

echo
if [[ ! $(git branch -r | grep $branch) ]]; then
  echo "Push the branch to remote before executing this action.";
  echo
  exit 1;
else
  echo "Make sure the branch is up to date in remote"
  echo
fi

run_id=$(curl -i -X POST \
  "$api_url/deployments/d87fc0e8-5c71-408a-bcbe-f2a6320686d1/create_flow_run" \
  -H "accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer $PREFECT_API_KEY" \
  -d "{\"parameters\": {\"git_branch\":\"$branch\"}}" | \
  sed -n 's/.*\([0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}\).*/\1/p')

flow="$base_url/flow-runs/flow-run/$run_id"
echo
echo "Flow Run can be accessed here: $flow";
echo

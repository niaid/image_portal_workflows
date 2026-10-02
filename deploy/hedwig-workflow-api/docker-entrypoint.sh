#!/bin/sh
set -e

USERNAME=$(python3 -c "import json, os; print(json.loads(os.environ.get('RDS_SECRETS_ARN', '{}')).get('username', ''))")
PASSWORD=$(python3 -c "import json, os; print(json.loads(os.environ.get('RDS_SECRETS_ARN', '{}')).get('password', ''))")

prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://$USERNAME:$PASSWORD@$RDS_ENDPOINT/$DATABASE"
prefect config set PREFECT_API_URL=https://$ALB_DNS_NAME/api
prefect config set PREFECT_UI_STATIC_DIRECTORY=/tmp/prefect-ui

export FORWARDED_ALLOW_IPS="*"

# Also, incoming env vars:
# prefect server start --host 0.0.0.0 --port $PREFECT_SERVER_API_PORT
uvicorn --host 0.0.0.0 --port $PREFECT_SERVER_API_PORT --factory server:create_auth_app --forwarded-allow-ips='*'

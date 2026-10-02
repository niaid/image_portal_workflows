# Hedwig workflow API deployment

This subtree contains the Prefect server and AWS infrastructure configuration
imported from `niaid/tf-hedwig-workflow-api`. The initial import preserves the
Dockerfile, server, requirements, entrypoint, and Spaces solution unchanged.
Source revision: `1c6cf711a98dff6a8ae3bd6bdf200e4616421874`.
The original repository has not been retired, and its Git history has not been
imported.

## Runtime boundaries

The server runs in an ECS container behind an ALB, with an RDS PostgreSQL
database. `server.py` adds authentication to the Prefect API. Spaces generates
Terraform from `hedwig.spaces-solution.yaml` using the Two-tier-webapp-ECS
solution.

The image-processing workflows remain in `em_workflows/` and run on HPC.
The server has its own Python 3.14 image and requirements; the workflow package
continues to use its existing Python 3.13 environment. Moving these files does
not deploy the HPC workers or change their entrypoints.

## GitHub Actions

The repository-root workflows are manually triggered:

- `Infrastructure Plan`: initialize and plan for dev, QA, or prod.
- `Infrastructure Deploy`: build, scan, and deploy for dev; promote the dev
  image to QA and deploy for QA; promote the QA image to prod and deploy for prod.

Both workflows run shell commands from this directory, use the existing
`cicd-runner-admin` Spaces role, and share a concurrency group per environment.
They are not triggered by pushes or pull requests. Plan is a separate review
step; Deploy still uses the original auto-approved apply task, not a saved plan.

## Local commands

Build only the server image from the repository root:

```sh
docker build -f deploy/hedwig-workflow-api/Dockerfile \
  -t hedwig-workflow-api:local deploy/hedwig-workflow-api
```

For authenticated Spaces operations, first change into this directory:

```sh
cd deploy/hedwig-workflow-api
export SPACES_SOLUTION_FILE=hedwig.spaces-solution.yaml
export SPACES_SOLUTION_ENV=dev
export SPACES_SOLUTION_ROLE=cicd-runner-admin
spaces task -- init
spaces task -- plan
```

Use `qa` or `prod` for the corresponding environment. Do not run apply or deploy
until the cutover checks below have passed. Generated Terraform, state, and
Spaces debug files must not be committed. The Docker context is limited to the
server build inputs.

## Cutover checklist

1. Configure GitHub environments `dev`, `qa`, and `prod` in this repository.
   Recreate required reviewers, deployment branch restrictions, secrets, and
   variables from the original repository before enabling deployments.
2. Make `NIAID_BUILD_AGENTS_SSH_KEY` and `NIAID_KNOWN_HOSTS` available to the
   jobs. Verify runner-group access for the existing `[self-hosted, linux]`
   runners, their Spaces credentials, and any repository-specific external
   authorization. Do not expose these runners or credentials to untrusted PRs.
3. Confirm that the imported solution selects the existing backend and remote
   state key for each environment. Preserve the `hedwig-workflow-api` space,
   `prefect2` stack, module/resource addresses, and existing image names. Do not
   create fresh state or migrate state just because the repository path changed.
4. Build the server image. Run Plan for each environment and compare with a
   plan from the original repository. Require no unexpected resource changes,
   particularly no database or service replacement, before any deployment.
5. Stop deployment dispatches in the original repository during cutover.
   GitHub concurrency groups do not coordinate between repositories.
6. Deploy dev, verify API health, authentication, and an HPC workflow run, then
   promote the tested image through QA and prod with the required approvals.
   Record the deployed image digest for rollback; database migrations may need
   a separate recovery plan.
7. Retire the original deployment workflows only after verification. Keep the
   original repository available for history and rollback investigation.

The server image build, authenticated environment plans, GitHub settings, and
live health checks are cutover gates, not consequences of copying the files.

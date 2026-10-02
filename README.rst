.. raw:: html

  <p align="center">
      <a href="https://github.com/niaid/image_portal_workflows/actions/workflows/main.yml" alt="Testing Source">
          <img alt="Python Tests" src="https://github.com/niaid/image_portal_workflows/actions/workflows/main.yml/badge.svg">
      </a>
      <a href="https://github.com/niaid/image_portal_workflows/actions/workflows/pages/pages-build-deployment" alt="GH Pages Build">
          <img alt="GH Pages Build" src="https://github.com/niaid/image_portal_workflows/actions/workflows/pages/pages-build-deployment/badge.svg" />
      </a>
      <br>
  </p>

Image Analysis Workflows, servicing NIAID's "Hedwig" project.

Please see our `Spinx Docs <https://niaid.github.io/image_portal_workflows/>`_ for details.

Prefect Server Infrastructure
----------------------------

The Prefect server image and Spaces/Terraform deployment configuration live in
``deploy/hedwig-workflow-api/``. See the
`deployment guide <deploy/hedwig-workflow-api/README.md>`_ for manual GitHub
Actions, local commands, and the migration cutover checklist. The HPC workflows
retain their existing package layout and deployment entrypoints.

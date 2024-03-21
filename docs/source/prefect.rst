Managing Prefect Server
=======================

Prefect Server is running, but workpool is not available
--------------------------------------------------------

In HPC, you can use the following command to create a work pool.

.. code-block::

   prefect work-pool create "workpool"

**Manual instruction:**

If the prefect server is running, but the workpool is not available, then you can create a workpool by going to the website where the prefect server is hosted. Go to `Work Pools` tab and create a workpool with the name `workpool`. This is the name of the workpool that is defined in [prefect.yaml > definitions > work_pools > name](https://github.com/niaid/image_portal_workflows/pull/353/files#diff-b49a6f022232810a70f1a0c2feffbbe84d018b2418a7996e52430c6063ada3a3R23) file.

Continuous Deployment (dev to qa to prod)
-----------------------------------------

Assuming that `dev` environment is working as expected, we can promote the environment to `qa` and `prod` as well.

The first thing we need to do is promote the image from `dev` to `qa`.

.. code-block::

   spaces task -f hedwig.spaces-solution.yaml promote-image -- dev qa

Afterwards, we can deploy the aws infrastructure for `qa` as,

.. code-block::

   SPACES_SOLUTION_ENV=qa spaces task -f hedwig.spaces-solution.yaml build-deploy

This can then again be applied for the `qa` to `prod` changes.

Managing Prefect Worker
=======================

Deploying workflows
-------------------


Once the prefect server is running and workpool is created. You can login to respective *BigSky* instance (dev, qa, prod) and deploy the workflows.

Make sure the configurations are correct:

0. Ensure prefect server `workpool` exists in the workpools tab on the appropriate Prefect Web console.

1. Update prefect.yaml to change the user and/or directory names

   .. code-block::

      # by default pull.directory setting is set for prod environment
      directory: /gs1/home/hedwig_prod/image_portal_workflows

      # change it to dev or qa, based on your environment

2. Check prefect config with view

   .. code-block::

      prefect config view

      # Update config iff required
      export PREFECT_API_KEY=xyz
      export PREFECT_API_URL=abc.com

3. Check HPC worker daemon:

   .. code-block::

      systemctl status hedwig_listener_prod


   Certain scenarios require the deamon to be restarted or reloaded, although typically we do not need to perform this step. (see helper_scripts/.service file) The service files should restart the worker if killed or on crash. 

4. Deploy flows with prefect deploy

   .. code-block::

      prefect deploy
      # Or deploy all based on prefect.yaml using the following setting
      # However, this will also deploy pytest_runner workflow in other envs (where it's not needed)
      # prefect deploy --all


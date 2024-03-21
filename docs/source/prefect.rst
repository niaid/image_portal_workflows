Managing Prefect Server
=======================


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

3. Deploy flows with prefect deploy

   .. code-block::

      prefect deploy
      # Or deploy all based on prefect.yaml using the following setting
      # However, this will also deploy pytest_runner workflow in other envs (where it's not needed)
      # prefect deploy --all

4. Run worker (properly via the helper_scripts/.service file)

   The service files should restarts the worker when killed. Normally, we would need to do this step


Troubleshooting:
--------------------------------------------------------

- Prefect Server is running, but workpool is not available

   In HPC, you can use the following command to create a work pool.
   `prefect work-pool create "workpool"`
   Enssure prefect server is running, and workpool is not available. If not create a workpool by going to the website where the prefect server is hosted. Go to `Work Pools` tab and create a workpool with the name `workpool`. This is the name of the workpool that is defined in prefect.yaml > definitions > work_pools > name file.

- IMOD unable to find `env`

.. code-block::

   Unable to run command.
   Cannot run program "env" (in directory "?"): error=2, No such file or directory

Note `directory "?"`, this implies that something is trying to run in a directory that does not exist. Ensure that the daemon is taken down, ensure that `ps aux | grep hedwig` does not list any processes that may be running, ensure that the service file is correct, ensure that the daemon is `reloaded` and `started`.



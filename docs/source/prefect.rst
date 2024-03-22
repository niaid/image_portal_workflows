Managing Prefect Server
=======================


Continuous Deployment (dev to qa to prod)
-----------------------------------------

Server images are promotoed from one environment to the next, i.e. `dev` -> `qa` -> `prod`.

For example to promote the image from `dev` to `qa`:

.. code-block::

   spaces task -f hedwig.spaces-solution.yaml promote-image -- dev qa

We can then deploy the aws infrastructure for `qa`:

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

2. Check HPC worker daemon:

   .. code-block::

      systemctl status hedwig_listener_prod

      
   Certain scenarios require the deamon to be restarted or reloaded, although typically we do not need to perform this step. (see helper_scripts/.service file) The `systemctl` should restart the worker if killed or on crash. 



3. Check prefect config with view

   .. code-block::

      prefect config view

      # Update config iff required
      export PREFECT_API_KEY=xyz
      export PREFECT_API_URL=abc.com

4. Deploy flows with prefect deploy

   .. code-block::

      prefect deploy
      # Or deploy all based on prefect.yaml using the following setting
      # However, this will also deploy pytest_runner workflow in other envs (where it's not needed)
      # prefect deploy --all



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


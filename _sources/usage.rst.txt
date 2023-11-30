==================
Submitting Jobs
==================

NOTE: You need to be in NIH VPN to be able to submit jobs for your workflow runs.

Workflow servers are deployed in `development <https://prefect2.hedwig-workflow-api.niaiddev.net>`_, `qa <https://prefect2.hedwig-workflow-api.niaidqa.net>`_, and `production <https://prefect2.hedwig-workflow-api.niaidprod.net>`_ environments separately.

Manual Submission
-----------------

Depending on the environment, go to the deployment section. Click on the `Kebab menu` icon next to your desired deployment and do a `Custom Run`. Here you can add appropriate values for the fields and submit your job. For manual submission, do not forget to add `no_api: true` value.

CLI/SDK Submission
---------------

You can also use `curl` to submit a job. The data body you post is same as the manual submission. However, you also need to have the information of `Deployment ID` which you can get from the workflow deployment's detail page.
Once you have the deployment id and relevant job information, you can submit a job as,

.. code-block::

   ~ curl -X 'POST' \
   'https://prefect2.hedwig-workflow-api.niaiddev.net/api/deployments/<DEPLOYMENT-ID>/create_flow_run' \
   -H 'accept: application/json' \
   -H 'Content-Type: application/json' \
   -d '{
   "parameters": {"key": value, "key2": value2, ...}
   }'

As same as curl, you can use any other SDK to submit a job. For example, you can use `axios` to `send a POST request <https://axios-http.com/docs/post_example>`_ to the workflow server to submit a job.

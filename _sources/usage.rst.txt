==================
Submitting Jobs
==================

NOTE: You need to be in NIH VPN to be able to submit jobs for your workflow runs.

Workflow servers are deployed in `development <https://prefect2.hedwig-workflow-api.niaiddev.net>`_, `qa <https://prefect2.hedwig-workflow-api.niaidqa.net>`_, and `production <https://prefect2.hedwig-workflow-api.niaidprod.net>`_ environments separately.

In order to submit a workflow job, you will need deployment IDs of each of the workflows, as shown:

.. list-table:: Deployment IDs (**Updated: 12/11/2023**)
   :widths: 20 25 25 25
   :header-rows: 1

   * - Pipelines
     - Development
     - QA
     - Production
   * - 2D / DM conversion
     - 68dd3d3e-6c5d-4616-ab33-52d4dcbd8184
     -
     -
   * - FIBSEM
     - 6eec994d-6d63-4e8e-b6fd-f4e516aa0caa
     -
     -
   * - BRT
     - 8a8f9c9b-476f-4b4c-8a19-881dc1cb5be9
     -
     -
   * - IF / Multichannel CZI
     - 265a8a6b-fa09-4503-a449-bdc5117933a5
     -
     -
   * - Large 2D PNG / ROI
     - 81f99df4-4de5-4f29-9b73-16dc9ee65b0c
     -
     -

Pipeline Parameters
-------------------

Below are the pipeline parameters for all the available workflows.

2D / DM conversion
``````````````````

.. code-block:: python

  {
    "type": "object",
    "title": "Parameters",
    "required": [
      "file_share",
      "input_dir"
    ],
    "properties": {
      "token": {
        "type": "string",
        "title": "token",
        "position": 4
      },
      "input_dir": {
        "type": "string",
        "title": "input_dir",
        "position": 1
      },
      "file_share": {
        "type": "string",
        "title": "file_share",
        "position": 0
      },
      "callback_url": {
        "type": "string",
        "title": "callback_url",
        "position": 3
      },
    }
  }

FIBSEM
```````

.. code-block:: python

  {
    "type": "object",
    "title": "Parameters",
    "required": [
      "file_share",
      "input_dir"
    ],
    "properties": {
      "token": {
        "type": "string",
        "title": "token",
        "position": 4
      },
      "input_dir": {
        "type": "string",
        "title": "input_dir",
        "position": 1
      },
      "file_share": {
        "type": "string",
        "title": "file_share",
        "position": 0
      },
      "tilt_angle": {
        "type": "number",
        "title": "tilt_angle",
        "default": 0,
        "position": 7
      },
      "callback_url": {
        "type": "string",
        "title": "callback_url",
        "position": 3
      }
    }
  }

BRT
```

.. code-block:: python

  {
    "type": "object",
    "title": "Parameters",
    "required": [
      "montage",
      "gold",
      "focus",
      "fiducialless",
      "trackingMethod",
      "TwoSurfaces",
      "TargetNumberOfBeads",
      "LocalAlignments",
      "THICKNESS",
      "file_share",
      "input_dir"
    ],
    "properties": {
      "gold": {
        "type": "integer",
        "title": "gold",
        "position": 1
      },
      "focus": {
        "type": "integer",
        "title": "focus",
        "position": 2
      },
      "token": {
        "type": "string",
        "title": "token",
        "position": 13
      },
      "montage": {
        "type": "integer",
        "title": "montage",
        "position": 0
      },
      "THICKNESS": {
        "type": "integer",
        "title": "THICKNESS",
        "position": 8
      },
      "input_dir": {
        "type": "string",
        "title": "input_dir",
        "position": 10
      },
      "file_share": {
        "type": "string",
        "title": "file_share",
        "position": 9
      },
      "TwoSurfaces": {
        "type": "integer",
        "title": "TwoSurfaces",
        "position": 5
      },
      "callback_url": {
        "type": "string",
        "title": "callback_url",
        "position": 12
      },
      "fiducialless": {
        "type": "integer",
        "title": "fiducialless",
        "position": 3
      },
      "adoc_template": {
        "type": "string",
        "title": "adoc_template",
        "default": "plastic_brt",
        "position": 16
      },
      "trackingMethod": {
        "type": "integer",
        "title": "trackingMethod",
        "position": 4
      },
      "LocalAlignments": {
        "type": "integer",
        "title": "LocalAlignments",
        "position": 7
      },
      "TargetNumberOfBeads": {
        "type": "integer",
        "title": "TargetNumberOfBeads",
        "position": 6
      }
    }
  }

IF / Multichannel CZI
``````````````````````

.. code-block:: python

  {
    "type": "object",
    "title": "Parameters",
    "required": [
      "file_share",
      "input_dir"
    ],
    "properties": {
      "token": {
        "type": "string",
        "title": "token",
        "position": 4
      },
      "input_dir": {
        "type": "string",
        "title": "input_dir",
        "position": 1
      },
      "file_share": {
        "type": "string",
        "title": "file_share",
        "position": 0
      },
      "callback_url": {
        "type": "string",
        "title": "callback_url",
        "position": 3
      }
    }
  }

Large 2d PNG / ROI
```````````````````

.. code-block:: python

  {
    "type": "object",
    "title": "Parameters",
    "required": [
      "file_share",
      "input_dir"
    ],
    "properties": {
      "token": {
        "type": "string",
        "title": "token",
        "position": 4
      },
      "input_dir": {
        "type": "string",
        "title": "input_dir",
        "position": 1
      },
      "file_share": {
        "type": "string",
        "title": "file_share",
        "position": 0
      },
      "callback_url": {
        "type": "string",
        "title": "callback_url",
        "position": 3
      }
    }
  }


The pipeline parameters can be also be observed programatically using following `curl` command:

.. code-block:: bash

   curl -X 'POST' 'https://prefect2.hedwig-workflow-api.niaiddev.net/api/deployments/filter' \
     -H 'accept: application/json' -H "Authorization: Bearer $PREFECT_API_KEY" -H 'Content-Type: application/json' -d '{}' \
       | jq -r '.[] | "\(.id)"' \
     | xargs -I{} \
     curl -X 'GET' 'https://prefect2.hedwig-workflow-api.niaiddev.net/api/deployments/{}' \
     -H 'accept: application/json' -H "Authorization: Bearer $PREFECT_API_KEY" \
       | jq '.description,.id,.parameter_openapi_schema' > schema.json


CLI/SDK Submission
---------------

You can use `curl` to submit a job.

.. code-block::

   ~ curl -X 'POST' \
   'https://prefect2.hedwig-workflow-api.niaiddev.net/api/deployments/<DEPLOYMENT-ID>/create_flow_run' \
   -H 'accept: application/json' \
   -H 'Content-Type: application/json' \
   -d '{
   "parameters": {"key": value, "key2": value2, ...}
   }'

As same as curl, you can use any other SDK to submit a job. For example, you can use `axios` to `send a POST request <https://axios-http.com/docs/post_example>`_ to the workflow server to submit a job.

Manual Submission
-----------------

Depending on the environment, go to the deployment section. Click on the `Kebab menu` icon next to your desired deployment and do a `Custom Run`. Here you can add appropriate values for the fields and submit your job. For manual submission, do not forget to add `no_api: true` value.

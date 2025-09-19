==================
Submitting Jobs
==================

NOTE: You need to be in NIH VPN to be able to submit jobs for your workflow runs.

Workflow servers are deployed in `development <https://prefect2.hedwig-workflow-api.niaiddev.net>`_, `qa <https://prefect2.hedwig-workflow-api.niaidqa.net>`_, and `production <https://prefect2.hedwig-workflow-api.niaidprod.net>`_ environments separately.

In order to submit a workflow job, you will need deployment IDs of each of the workflows, as shown:

.. list-table:: Deployment IDs (**Updated: 01/26/2023**)
   :widths: 20 25 25 25
   :header-rows: 1

   * - Pipelines
     - Development
     - QA
     - Production
   * - 2D / DM conversion
     - b8b3854d-396a-4b92-ad91-48f3ff32b18a
     - 20ab41f5-3770-4834-97d0-33ce2f2d7c3e
     - a72f8c80-9c51-4b50-b2e5-1d27314bcccb
   * - FIBSEM
     - 46ac3986-3d58-4a7c-9ad6-32b73a0c418f
     - 32ed45f3-37eb-4b34-8220-f52fc9c96fb7
     - 9d3f7d8d-308f-4d95-9433-66ae253a6379
   * - BRT
     - d1ad7a1e-c64a-4f7f-9af2-f065dd2b0a4c
     - 6fcb4637-878f-4630-9f89-2bdea430ac3e
     - cdf92549-80d9-4f4e-bde7-30409ed20e41
   * - IF / Multichannel CZI
     - c2f50018-7d11-4535-8c6e-b039bfa025bd
     - 208568ff-f487-4e10-bbd2-208640d39143
     - dccb9b62-cd2e-489d-ac12-f35b76b91fb1
   * - Large 2D PNG / ROI
     - 432592af-0899-4425-94b4-c239614b1748
     - 8873c33b-d46d-4296-ae4f-0157ab2f5911
     - e5b25df2-6ae0-49f6-a080-cbe7c32e4f6a

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
      "file_name": {
        "type": "string",
        "title": "file_name",
        "position": 2
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


Pipeline Response
-----------------

All pipelines responses follow the same schema. Currently, all schema are documented in yaml files as such:

.. literalinclude:: ../../api_schema/PipelineCallback.yaml
   :language: yaml
   :emphasize-lines: 1-5,13-21
   :linenos:

You can explore more on the yaml files in the `Github <https://github.com/niaid/image_portal_workflows/tree/main/api_schema>`_ repo.

CLI/SDK Submission
------------------

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

####################
Project Development
####################


We assume you are developing locally, on Linux or Mac OS, with access to Python 3.10 (or later).


*************
Local Set-up
*************

Here we will set up our virtual environment "dev", and install workflows source.

.. code-block:: sh

   # create env
   python3 -m venv dev

   # activate env
   source dev/bin/activate

   # add pipelines to this env
   pip install -e . -r requirements.txt --upgrade


********************************
Required utils and ``.env`` file
********************************

These pipelines rely on a number of freely available utilities:

- Java runtime (in order to run `etomo` in BRT)
- ffmpeg (in order to create movies)
- IMOD package: https://bio3d.colorado.edu/imod/
- bioformats2raw package: https://github.com/glencoesoftware/bioformats2raw
- imagemagick: https://imagemagick.org/script/download.php

The pipelines use the `.env` file, located in the project root directory, to path these utilities.
For convenience the file `.env.sample` is provided, and can be used by renaming it to `.env`.
You can then ensure the paths in this file are appropriate to your system.


.. code-block:: sh

   mv .env.sample .env

   # config paths in file
   vim .env



Similar to the HPC Set up below, you can locally set up `dev` and `qa` virtual envs.

Special note for **Mac M1**: The `tomojs-pytools` library depends on imagecodecs which does
not have binaries built for the M1. Need to install using an x86_64 version of Python. Also, there
are issues installing `biofomats2raw` as there is no OpenCV package for arm64 chip. There currently
is no fix or workaround for this issue.





*******
Testing
*******

All ``pytest`` files reside in the `test` directory:

- ``test_brt``: end-to-end test of batchruntomo pipeline
- ``test_dm``: 2D end-to-end pipeline test
- ``test_sem``: end-to-end test of FIBSEM pipeline
- ``test_lrg_2d``: Large 2d pipeline test
- ``test_czi``: Spatial Omics (IF - immunofluoroscene) pipeline test
- ``test_utils``: unit tests of utils/utils.py module

There is test data for `test_dm` in the Git repo, but not for the others. These files need to be
downloaded from the HPC machines. The following script will copy them:

`test/copy_test_data.sh`

These files are quite large, so you may want to run each line separately at the command line.

Some unit tests also require the results of previous ``test_brt`` runs, specifically in the Assets directory. So you must run ''test_brt''
before the complete test suite will work.

To run the entire test suite, in the portal_image_workflows directory, run::

    $ pytest

To determine the test coverage, in the portal_image_workflows directory, run::

    $ pytest --cov="." --cov-config=.coveragerc test

There are also a couple ways to select specific types of tests based on pytest `markers
<https://docs.pytest.org/en/7.1.x/example/markers.html#registering-markers>`_.
Certain tests use the following decorators to "mark" it:

    - ``@pytest.mark.slow`` : Test takes a long time to run
    - ``@pytest.mark.localdata`` : Test requires large test file not stored in the repository

So to run tests in GitHub Actions that don't have all the data, run::

    $ pytest -m "not localdata"

To run just run relatively quick tests, run::

    $ pytest -m "not slow"

To run everything, along with verbose pytest output and logging turned on, run::

    $ pytest -v --log-cli-level=INFO

You can also run tests by "keyword" based on the name of the test. For example, the following will run two
tests in ``test_utils.py`` named ``test_lookup_dims`` and ``test_bad_lookup_dims``::

    $ pytest -k lookup_dims

In addition, temporarily "broken" tests are marked with ``@pytest.mark.skip``. These will be skipped
every time; comment out or delete the decorator to run them.


Docker and ``.env`` file
========================

You can also choose to use Docker for local development and testing.
Note, the container will need an `.env` file similar to any other environment. The easiest way
to achieve this is copying the existing example file `.env.sample` to `.env`, this should work
within the container as is.

To build the docker image, use `--platform linux/amd64` option.
Explanation can be found `here <https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.wiki/tab::5f55363b-bb53-4e5b-9564-8bed5289fdd5?context=%7B%22subEntityId%22%3A%22%7B%5C%22pageId%5C%22%3A15%2C%5C%22sectionId%5C%22%3A17%2C%5C%22origin%5C%22%3A2%7D%22%2C%22channelId%22%3A%2219%3A869be6677ee54848bc13f2066d847cc0%40thread.skype%22%7D&tenantId=14b77578-9773-42d5-8507-251ca2dc2b06>`_

The basic usage for testing would look like below. The command assumes that you are running the container from the project directory where the main Dockerfile is located.

.. code-block:: sh

   # To build the image
   docker build . -t hedwig_pipelines --platform linux/amd64


   # To run the container of that image
   docker run -v "$(pwd):/image_portal_workflows" -e USER=root -it --rm hedwig_pipelines:latest

I don't think this is still true??
Note that we are setting a USER environment variable here. This is because `class Config` requires this environment variable set.




From within the container command can be run normally, for example: `pytest`.

***********************
Contributing and GitHub
***********************

We host our source code and docs on Github: https://github.com/niaid/image_portal_workflows
and https://niaid.github.io/image_portal_workflows

If you would like to contribute, please see:
https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request
along with: https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project


pre-commit
==========
We use `pre-commit <https://pre-commit.com/>`_, a framework for managing pre-commit hooks that will check code for code
format compliance at commit time. It is configured via a top-level ``.pre-commit-config.yaml`` file that runs ``black``,
``Flake8`` and other checks.


Thanks! :)

*************
Documentation
*************

`Sphinx <https://www.sphinx-doc.org/>`_ documentation as automatically rendered and pushed the the gh-pages branch. The
API is documented in Sphinx from the the Python docstring automatically for the public module methods and select private
methods.

to create docs locally
(note this will continually update the docs as they are saved, where they can be viewed on your browser):

.. code-block:: sh

   # make sure you're in the docs dir
   pwd
   /home/macmenaminpe/image_portal_workflows/docs


   # clean up anything from last time and create live docs
   rm -rf _build && make livehtml
   [I 231205 20:33:41 server:335] Serving on http://0.0.0.0:34733
   [I 231205 20:33:41 handlers:62] Start watching changes
   [I 231205 20:33:41 handlers:64] Start detecting changes
   [I 231205 20:33:53 handlers:135] Browser Connected: http://0.0.0.0:34733/



*************
HPC Set up
*************

**NOTE, THIS IS ONLY relevant for HPC. Added for completeness.**

**NOTE: generate_vevn.sh is not used as of 08/11/2023, setup is documented in** :doc:`hpc`.

Workflows are currently run on RML HPC ("BigSky").

There are three environments currently on BigSky: (`dev`, `qa`, `prod`).
They were set up as follows:
(Note, this first step is only required once, and only to work around ancient versions of Python.)

.. code-block:: sh

   # Obtain and set up Miniconda (to allow setting up of venvs) e.g.
   wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.11.0-Linux-x86_64.sh
   # Activate conda to create the virtual environment
   conda create -n conda_env
   conda activate conda_env
   # create venv, "some_name" for example
   python3 -m venv some_name
   # don't need the conda env any more...
   conda deactivate



.. code-block:: sh

   git clone git@github.com:niaid/image_portal_workflows.git
   # generates venv qa.
   ./image_portal_workflows/helper_scripts/generate_venv.sh qa


Updating venvs:
---------------
To update your python virtual environment:
Change into the correct venv directory, e.g. `~/code/hedwig/dev/image_portal_workflows`.
Ensure environment is active and run something like:

`pip   install -e . -r requirements.txt --upgrade  --find-links https://github.com/niaid/tomojs-pytools/releases/tag/v1.3`


Prefect Agent:
--------------
The prefect agent, the thing that reaches out to the prefect API machine, is daemonized on HPC.
See `image_portal_workflows/helper_scripts/hedwig_reg_listen.sh` and
`image_portal_workflows/helper_scripts/hedwig_listener_prod.service` etc.


Register Workflows:
-------------------
To register a new workflow, or update an existing one (in `qa` environment):
(The workflow needs to be registered every time the source is updated.)
`image_portal_workflows/helper_scripts/hedwig_reg_listen.sh qa register`


Currently dask jobqueue is configured with a yaml file.

.. code-block:: sh

   $ cat ~/.config/dask/jobqueue.yaml
   # Dask worker options
   cores: 8                # Total number of cores per job
   memory: "64 GB"                # Total amount of memory per job
   processes: 1                # Number of Python processes per job

   # interface: ens160           # Network interface to use like eth0 or ib0
   death-timeout: 120           # Number of seconds to wait if a worker can not find a scheduler
   local-directory: /gs1/home/macmenaminpe/tmp       # Location of fast local storage like /scratch or $TMPDIR
   job_extra_directives: ["--gres=gpu:1"]

   # SLURM resource manager options
   shebang: "#!/usr/bin/env bash"
   queue: gpu
   project: null
   walltime: '10:00:00'

- Note, although unused above, BigSky also has Spack available.

.. code-block:: sh

  $ source /gs1/apps/user/rmlspack/share/spack/setup-env.sh
  $ spack load -r python@3.8.6/eg2vaag
  $ python -V
  Python 3.8.6
  $ spack unload -a




Git LFS
=======

*We are aiming to use s3 (with rsync capabilities) over git-lfs for test data storage*

###########
Development
###########

The docs related to development and testing are contained in this section.

*************
Prerequisites
*************

Please read this Development section before cloning. The `generate_venv.sh` script referenced below
automates much of the process. Current system assumes that you are locally running either a Linux
or Mac OS.

Github.com
==========

The repository is located in the NIAID Github.com enterprise organization. Having a github.com account
which is a member of the NIAID organization is required.

pip install -e <path_to_clone>
git+https://github.com/niaid/tomojs-pytools.git@master in requirements.txt
￼

Local Set-up
============

You will need a minimal Python (3.8+) installation to run the `generate_venv.sh` script.
The following programs need to be available locally:

- Java runtime (in order to run `etomo` in BRT)
- ffmpeg (in order to create movies)
- IMOD package: https://bio3d.colorado.edu/imod/
- bioformats2raw package: https://github.com/glencoesoftware/bioformats2raw

Similar to the HPC Set up below, you can locally set up `dev` and `qa` virtual envs.

Special note for **Mac M1**: The `tomojs-pytools` library depends on imagecodecs which does
not have binaries built for the M1. Need to install using an x86_64 version of Python. Also, there
are issues installing `biofomats2raw` as there is no OpenCV package for arm64 chip. There currently
is no fix or workaround for this issue.

HPC Set up
==========

NOTE, THIS IS **ONLY** relevant for HPC. Added for completeness.
----------------------------------------------------------------

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


A script exists to help set up `dev`, `qa`, or `prod` environments in
`$HOME/code/hedwig/<HEDWIG_ENV>`
Insure `$HOME/code/hedwig` exists. Runs on Linux.

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

Git `Large File Storage <https://git-lfs.github.com>`_ (LFS) is used to store larger files in the repository such as
test images, trained models, and other data ( i.e. not text based code ). Before the repository is cloned, git lfs must
be installed on the system and set up on the users account. The `tool's documentation <https://git-lfs.github.com>`_
provides details on installation, set up, and usage that is not duplicated here. Once set up the git usage is usually
transparent with operation such as cloning, adding files, and changing branches.

The ``.gitattributes`` configuration file automatically places files in the ``test/input_files`` directory to
be stored in Git LFS.

Flake8
======

`Flake8 <https://pypi.org/project/flake8/>`_ is a CLI utility used to enforce style and do linting.

Black
=====

`Black <https://pypi.org/project/black/>`_ is a CLI utility that enforces a standard formatting on Python which helps with code consistancy.

pre-commit
==========

`pre-commit <https://pre-commit.com/>`_ is a framework for managing pre-commit hooks that will check code for code
format compliance at commit time. It is configured via a top-level ``.pre-commit-config.yaml`` file that runs ``black``,
``Flake8`` and other checks.

*****************
Pull Requests
*****************

To contribute to the project first ensure your fork is in good shape, and then the generate a Pull Request (PR) to the **niaid** fork. Below is an outline an of the kind of steps that could be followed. More thorough documentation can be found here: https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request

- Fork niaid repo into your gh account using the web interface.

- Clone your repo to local machine, e.g.::

    git clone git@github.com:philipmac/nih_3d_workflows.git

- Set `upstream`::

    git remote add upstream git@github.com:niaid/nih_3d_workflows.git

- ensure origin and upstream look something like this::

   $ git remote -v
   origin	git@github.com:your_uname/image_portal_workflows.git (fetch)
   origin	git@github.com:your_uname/image_portal_workflows.git (push)
   upstream	git@github.com:niaid/image_portal_workflows.git (fetch)
   upstream	git@github.com:niaid/image_portal_workflows.git (push)

- Make edits to local copy.

- Run `flake8`::

    flake8 . --max-line-length=127

- Run `Black`::

    black .

- Ensure neither `black` nor `flake8` are complaining.

- Commit your local work, ensure you're up to date with `upstream`, and push to `origin`::

    git commit -m "Fixes issue 123, ..."
    git fetch upstream
    git rebase upstream/master
    git push origin branch_with_fix

- Initiate creation the Pull Request (PR) via your fork into niaid/nih-3d-main using the web interface.

- Look at your changes, ensure *only* those changes are included in your PR.

- Submit PR with some helpful English. See: https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project

- Feel free to let a niaid repo admin (currently Philip MacM and Bradley Lowenkamp) know there's a PR waiting for review.

Thanks! :)

*******
Testing
*******

There are currently four pytest files in the `test` directory:

- test_brt: end-to-end test of batchruntomo pipeline
- test_dm: 2D end-to-end pipeline test
- test_sem: end-to-end test of FIBSEM pipeline
- test_utils: unit tests of utils/utils.py module

There is test data for `test_dm` in the Git repo, but not for the other two. These files need to be
downloaded from the HPC machines. The following script will copy them:

`test/copy_test_data.sh`

These files are quite large, so you may want to run each line separately at the command line. Some unit tests also require
the results of previous ``test_brt`` runs, specifically in the Assets directory. So you must run test_brt before the complete
test suite will work.

To run the entire test suite, in the portal_image_workflows directory, run:

.. code-block::

    $ pytest test

To determine the test coverage, in the portal_image_workflows directory, run:

.. code-block::

    $ pytest --cov="." --cov-config=.coveragerc test



********************
Sphinx Documentation
********************

`Sphinx <https://www.sphinx-doc.org/>`_ documentation as automatically rendered and pushed the the gh-pages branch. The
API is documented in Sphinx from the the Python docstring automatically for the public module methods and select private
methods.

==================
RML HPC (Big Sky)
==================

*******************
Python environments
*******************

**NOTE, THIS IS ONLY relevant for HPC. Added for completeness.**

**NOTE: generate_vevn.sh is not used as of 08/11/2023, setup is documented in** :doc:`hpc`.

Workflows are currently run on RML HPC ("BigSky").

There are three environments currently on BigSky: (`dev`, `qa`, `prod`).
They were set up as follows:
(Note, this first step is only required once, and only to work around old versions of Python.)


Initial Setup:
--------------

To initialise, typically done only at start per env. Use miniconda to set up venv, clone this repo, and pip install to the newly created venv.

.. code-block:: sh

   # get miniconda dist
   wget https://repo.anaconda.com/miniconda/Miniconda3-py310_23.3.1-0-Linux-x86_64.sh
   chmod a+x Miniconda3-py310_23.3.1-0-Linux-x86_64.sh
   ./Miniconda3-py310_23.3.1-0-Linux-x86_64.sh
   # use to create venv, eg prod
   /gs1/home/hedwig_prod/miniconda3/bin/python -m venv prod
   # get this repo
   git clone git@github.com:niaid/image_portal_workflows.git
   # activate the venv, and install in editable mode (--e)
   source prod/bin/activate
   cd image_portal_workflows/
   python -m pip install -e .


Note ``ls ~`` should list you ``image_portal_workflows``


Source being root of HOME is for convenience, and allows each serivce account to look similar.

Set up bashrc to automate venv activation.
Added these lines to ``.bashrc``

.. code-block::

  export HEDWIG_ENV=prod
  source prod/bin/activate


Set up daemon to use appropriate service file. You will need to email NIAID RML HPC Support to do this.
E.g. for qa ``helper_scripts/hedwig_listener_qa.service`` would be copied into place.


It's a good idea to test the ``ExecStart`` command can be run, eg:

``image_portal_workflows/helper_scripts/hedwig_listener_dev.service.sh``

The daemon by default polls prefect server in the given workpool and brings in the flow run details if something
has been submitted to the server.

To update:
----------
Tag relase, and test in dev etc.
Upon promotion into HPC env do:

.. code-block::

   cd image_portal_workflows/
   git fetch
   git checkout <label>
   pip install -e . -r requirements.txt --upgrade

When there are changes in the workflows (e.g, a new task is added, task function signature has changed, etc), you should
redeploy the workflows. See Prefect section for details.

*********************
NFS Filesystem layout
*********************

Pipeline inputs and outputs are housed on an Admin User accessable NFS, meaning that the Admin level users of the system can see directly into the same partitions which we read our inputs and ultimately place our outputs. Note, we do not do "work" in this Filesystem, only read in initial inputs, and write out selected outputs.
Different projects have different filesystem layouts.

Spatialomics file layout.
-------------------------

Normally the dir structure is : ``$lab/$pi/$project/$session/$sample``

For Spatialomics this is not the case, the $sample is not really a sample, it's grouping of ROIs, PreROIs, etc from each of the slides.

More details can be found in :ref:`ref-workflow-spatial-omics`.


- Note, although unused above, BigSky also has Spack available.

.. code-block:: sh

  $ source /gs1/apps/user/rmlspack/share/spack/setup-env.sh
  $ spack load -r python@3.8.6/eg2vaag
  $ python -V
  Python 3.8.6
  $ spack unload -a

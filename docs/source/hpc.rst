==================
RML HPC (Big Sky)
==================


Initial Setup:
--------------

To initialise, typically done only at start per env. Use miniconda to set up venv, clone this repo, and pip install to the newly created venv.

.. code-block::

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

``image_portal_workflows/helper_scripts/hedwig_reg_listen.sh listen``


To update:
----------
Tag relase, and test in dev etc.
Upon promotion into HPC env do:

.. code-block::

   cd image_portal_workflows/
   git fetch
   git checkout <label>
   python -m pip install -e .


Finally register the worlfows with the helper script.

``./helper_scripts/hedwig_reg_listen.sh register``



Spatialomics file layout.
-------------------------

Normally the dir structure is : ``$lab/$pi/$project/$session/$sample``

For Spatialomics this is not the case, the $sample is not really a sample, it's grouping of ROIs, PreROIs, etc from each of the slides.

More details can be found in :ref:`ref-workflow-spatial-omics`.

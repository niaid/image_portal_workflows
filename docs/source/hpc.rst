==================
RML HPC (Big Sky)
==================


Setup:
-------

HPC doesn't seem to have a current version of Python available. Need to get something set up to init the ``venvs``, chose conda.
Install conda as normal (in this case anaconda, should probably use miniconda instead).
Activate once to allow env generation. Don't allow conda update bashrc, don't want to run conda upon login.

Load the conda env: 

``eval "$(/gs1/home/hedwig_dev/anaconda3/bin/conda shell.bash hook)"``

Then create the venv, eg ``dev``

``python3 -m venv dev``

Deactivate conda, we shouldn't need it any more. (This is only needed upon initialization, one time.)

``conda deactivate``

Activate the venv.

``source ~/dev/bin/activate``

clone the repo to the root of home.

``git clone git@github.com:niaid/image_portal_workflows.git``

Now ``ls ~`` should list you ``image_portal_workflows``

Then get dependencies for project:

.. code-block::

  cd image_portal_workflows/
  python -m pip install -e .


Set up bashrc to facilitate environment set up.
Added these lines to ``.bashrc``

.. code-block::

  export HEDWIG_ENV=qa
  source qa/bin/activate


Set up daemon to use appropriate service file. You will need to email NIAID RML HPC Support to do this.
E.g. for qa ``helper_scripts/hedwig_listener_qa.service`` would be copied into place.


It's a good idea to test the ``ExecStart`` command can be run, eg:

``image_portal_workflows/helper_scripts/hedwig_reg_listen.sh listen``


To update source:
~~~~~~~~~~~~~~~~~
Tag your relase, and test in dev etc.
Upon promotion into HPC env do:

``cd image_portal_workflows/``
and
``git checkout <label>``

Once you're happy this is in order, register the worlfows with the helper script.

``./helper_scripts/hedwig_reg_listen.sh register``



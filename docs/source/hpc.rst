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

Clone the repo to the root of home. 

``git clone git@github.com:niaid/image_portal_workflows.git``

Now ``ls ~`` should list you ``image_portal_workflows``


Now get dependencies for project:

.. code-block::

  cd image_portal_workflows/
  python -m pip install -e .

Source being root of HOME is for convenience, and allows each serivce account to look similar. We need to install *this* repo into our venv. To do this tell ``pip`` the location of ``setup.py`` within the activated venv, eg:

``python -m pip install ~/image_portal_workflows/``
 
An alternative setup could be run, similar to pytools, whereby source is retrieved via a git label or branch name via `requirements.txt`. I felt that having a complete git repo available in HOME of each account offered more flexibility.

Set up bashrc to automate venv activation.
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
Tag relase, and test in dev etc.
Upon promotion into HPC env do:

.. code-block::

   cd image_portal_workflows/
   git fetch
   git checkout <label>


Once happy this is in order, register the worlfows with the helper script.

``./helper_scripts/hedwig_reg_listen.sh register``



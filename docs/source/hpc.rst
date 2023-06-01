==================
RML HPC (Big Sky)
==================


Usage:
-------

Using the cluster information.

Setup:
-------

HPC doesn't seem to have a current version of Python available. Need to get something set up to init the venvs, chose conda.
Install conda as normal (in this case anaconda, should probably use miniconda instead).
Activate once to allow env generation. Don't allow conda update bashrc, don't want to run conda upon login.

Load the conda env: 

`eval "$(/gs1/home/hedwig_dev/anaconda3/bin/conda shell.bash hook)"`

Then create the venv, eg `dev`

`python3 -m venv dev`

Deactivate conda, we shouldn't need it any more.

`conda deactivate`

Activate the venv.

`source ~/dev/bin/activate`
(and add this to .bashrc to load automatically.)

clone the repo to the root of home.

`git clone git@github.com:niaid/image_portal_workflows.git`

`cd image_portal_workflows/`
`python -m pip install -e .`




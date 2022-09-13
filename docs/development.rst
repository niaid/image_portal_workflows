###########
Development
###########

The docs related to development and testing are contained in this section.

*************
Prerequisites
*************

Please read before cloning.

Github.com
==========

The repository is located in the NIAID Github.com enterprise organization. Having a github.com account which is a member of the NIAID organization is required.

HPC Set up
==========

Workflows are currently run on RML HPC ("BigSky").



There are three environments currently on BigSky: (`dev`, `qa`, `prod`).
They were set up as follows:

- Obtain and set up Miniconda (to allow setting up of venvs) e.g.::

  wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.11.0-Linux-x86_64.sh


- Activate conda to create the virtual environment::

  conda create -n conda_env
  conda activate conda_env
  # create dev for example
  python3 -m venv code/hedwig_venv_dev
  # don't need the conda env any more...
  conda deactivate


- Set up dev venv.::

  source ~/code/hedwig_venv_dev/bin/activate
  cd ~/code/hedwig_venv_dev
  git clone git@github.com:niaid/image_portal_workflows.git
  cd image_portal_workflows/
  pip3 install -r requirements.txt --find-links https://github.com/niaid/tomojs-pytools/releases/tag/v1.0
  # ensure PYTHONPATH isn't set!
  unset  PYTHONPATH
  # ensure HEDWIG_ENV is set!
  export HEDWIG_ENV=dev
  # there's an issue with looking up certs - this fixes it
  export REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
  # start an agent specific to this env (project), note agent label "dev".
  prefect agent local start -l qa --key the_key
  # stop, background, and disown agent - note this will be deamonized.
  <crtl> z
  bg
  disown "%prefect"
  prefect create project "RML_QA"
  cd em_workflows
  pip install -e .
  # register each of the workflows, for example:
  prefect register --project "RML_DEV" -p em_workflows/dm_conversion/
  prefect register --project "RML_DEV" -p em_workflows/brt/
  # ...etc per workflow
  # (The workflow needs to be registered every time the source is updated.)


- Note, although unused above, BigSky also has Spack available, e.g.::

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

The ".gitattributes" configuration file automatically places files in the directories "test/data" and "cxr_similarity/data" to
be stored in Git LFS.

Flake8
======

`Flake8 <https://pypi.org/project/flake8/>`_ is a CLI utility used to enforce style and do linting. 

Black
=====

`Black <https://pypi.org/project/black/>`_ is a CLI utility that enforces a standard formatting on Python which helps with code consistancy. 

*****************
Pull Requests
*****************

To contribute to the project first ensure your fork is in good shape, and then the generate a Pull Request (PR) to the `niaid` fork. Below is an outline an of the kind of steps that could be followed. More thorough documentation can be found here: https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request

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

- Thanks! :)

*******
Testing
*******

TODO

********************
Sphinx Documentation
********************

`Sphinx <https://www.sphinx-doc.org/>`_ documentation as automatically rendered and pushed the the gh-pages branch. The
API is documented in Sphinx from the the Python docstring automatically for the public module methods and select private
methods.

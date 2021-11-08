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

The repository is located in the NIAID Github.com enterprise organization. Having a github.com account which is a member
of the NIAID organization is required.

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

A CLI utility used to enforce style and do linting, avaiable `here <https://pypi.org/project/flake8/>`_.
Note: Github actions is set to allow longer lines i.e.: `flake8 . --max-line-length=127`

Black
=====

Python formatting can be tedius, the `black` formatter enforces formatting consistancy. Black is a CLI utility available `here <https://pypi.org/project/black/>`_.
Black is run within Github Actions as `black .`

*****************
Development Setup
*****************

Generally speaking we want to get our personal forks in good shape prior to creating a Pull Request (PR) to the `niaid` fork. The below outlines an idea of the kind of process that might be followed. Better documentation can be found here: https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request

- Fork niaid repo into your gh account via web interface.

- Clone your repo to local machine, e.g.:

	`git clone git@github.com:philipmac/nih_3d_workflows.git`

- Set `upstream` as niaid repo.

	`git remote add upstream git@github.com:niaid/nih_3d_workflows.git`

- ensure origin and upstream look something like this:

  ```
  $ git remote -v
  origin	git@github.com:your_uname/image_portal_workflows.git (fetch)
  origin	git@github.com:your_uname/image_portal_workflows.git (push)
  upstream	git@github.com:niaid/image_portal_workflows.git (fetch)
  upstream	git@github.com:niaid/image_portal_workflows.git (push)
  ```

- Make edits to local copy.

- Run `flake8` linter (see https://pypi.org/project/flake8/):
          `flake8 . --max-line-length=127`

- Run `black` formatter (see https://pypi.org/project/black/).

          `black .`

- Ensure neither `black` nor `flake8` are complaining.

- Commit your local work, ensure you're up to date with `upstream`, and push to `origin`.

	```
	git commit -m "Fixes issue 123, ..."
	git fetch upstream
	git rebase upstream/master
	git push origin branch_with_fix
	```

- Initiate creation the Pull Request (PR) via your fork into niaid/nih-3d-main using the web interface.

- Look at your changes, ensure *only* those changes are included in your PR.
  
- Submit PR with some helpful English. See: https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project

- Feel free to let niaid repo admin (currently Philip MacM) know there's a PR waiting for review.

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

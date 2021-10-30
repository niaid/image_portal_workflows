# image_portal_workflows
Workflows related to project previously referred to as "Hedwig"


# How to contribute:
For a more detailed example, see: https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request
Broadly speaking the below should be close to what you'll need to do.

- Fork niaid repo into your gh account via web interface.
- Clone your repo to local machine, e.g.:

	`git clone git@github.com:philipmac/nih_3d_workflows.git`

- Set `upstream` as niaid repo.

	`git remote add upstream git@github.com:niaid/nih_3d_workflows.git`

- ensure origin and upstream look something like this:
  ```
  $ git remote -v
  origin	git@github.com:philipmac/image_portal_workflows.git (fetch)
  origin	git@github.com:philipmac/image_portal_workflows.git (push)
  upstream	git@github.com:niaid/image_portal_workflows.git (fetch)
  upstream	git@github.com:niaid/image_portal_workflows.git (push)
  ```
- Make changes to local copy.
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

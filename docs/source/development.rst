Project Development
===================


We assume you are developing locally, on Linux or Mac OS, with access to Python 3.10 (or later).


Virtual environment Set-up
~~~~~~~~~~~~~~~~~~~~~~~~~~

Set up virtual environment, for example "dev", and install workflows source.

.. code-block:: sh

   # create env
   python3 -m venv dev

   # activate env
   source dev/bin/activate

   # add pipelines to this env
   pip install -e . -r requirements.txt --upgrade

Similar to the HPC Set up below, you can locally set up `dev` and `qa` virtual envs.


Ancillary utils and ``.env`` file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Our pipelines rely on a number of freely available utilities:

- Java runtime (in order to run `etomo` in BRT)
- ffmpeg (in order to create movies)
- `IMOD <https://bio3d.colorado.edu/imod/>`_
- `bioformats2raw <https://github.com/glencoesoftware/bioformats2raw>`_
- `imagemagick: <https://imagemagick.org/script/download.php>`_
- `GraphicsMagick: <http://www.graphicsmagick.org>`_  Can be installed from condaforge.

.. note::
   The ``.env`` file
   -----------------
   In order to path the location of utilities like the ones mentioned above the pipelines use the `.env` file.
   This file located in the project root directory, and for convenience an example `.env.sample` is provided.
   To use this example it should be renamed it to `.env`, and you can then ensure the paths listed are appropriate to your system.

   .. code-block:: sh

      #copy example for use
      cp .env.sample .env

      # config paths in file
      vim .env



.. note::

        Special note for **Mac M1**: The `tomojs-pytools` library depends on imagecodecs which does
        not have binaries built for the M1. Need to install using an x86_64 version of Python. Also, there
        are issues installing `biofomats2raw` as there is no OpenCV package for arm64 chip. There currently
        is no fix or workaround for this issue.


Docker and ``.env`` file
~~~~~~~~~~~~~~~~~~~~~~~~

You can also choose to use Docker for local development and testing.
Note, the container will need an `.env` file similar to any other environment. The easiest way
to achieve this is copying the existing example file `.env.sample` to `.env`, this should work
within the container as is.

To build the docker image, use `--platform linux/amd64` option.
Explanation can be found `here <https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.wiki/tab::4f55363b-bb53-4e5b-9564-8bed5289fdd5?context=%7B%22subEntityId%22%3A%22%7B%5C%22pageId%5C%22%3A15%2C%5C%22sectionId%5C%22%3A17%2C%5C%22origin%5C%22%3A2%7D%22%2C%22channelId%22%3A%2219%3A869be6677ee54848bc13f2066d847cc0%40thread.skype%22%7D&tenantId=14b77578-9773-42d5-8507-251ca2dc2b06>`_

The basic usage for testing would look like below. The command assumes that you are running the container from the project directory where the main Dockerfile is located.

.. code-block:: sh

   # To build the image
   docker build . -t hedwig_pipelines --platform linux/amd63

   # Create a container using that image
   docker run -v "$(pwd):/image_portal_workflows" -it --rm hedwig_pipelines:latest

   # run something inside the container
   root@f3a8b5823032:/image_portal_workflows$ pytest test/test_utils.py



Testing
~~~~~~~

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




Contributing and GitHub
~~~~~~~~~~~~~~~~~~~~~~~

`Source code <https://github.com/niaid/image_portal_workflows>`_
and `Docs <https://niaid.github.io/image_portal_workflows>`_

While contributing find useful information on: `creating a PR <https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request>`_
along with `contributing to a project <https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project>`_.


pre-commit
----------
We use `pre-commit <https://pre-commit.com/>`_, a framework for managing pre-commit hooks that will check code for code
format compliance at commit time. It is configured via a top-level ``.pre-commit-config.yaml`` file that runs ``black``,
``Flake8`` and other checks.


Thanks! :)


Sphinx Documentation
~~~~~~~~~~~~~~~~~~~~

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




Register Workflows:
~~~~~~~~~~~~~~~~~~~

To register a new workflow, or update an existing one (in `qa` environment):
(The workflow needs to be registered every time the source is updated.)
`image_portal_workflows/helper_scripts/hedwig_reg_listen.sh qa register`


Git LFS
~~~~~~~

*We are aiming to use s3 (with rsync capabilities) over git-lfs for test data storage*

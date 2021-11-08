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

*****************
Development Setup
*****************

TODO

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

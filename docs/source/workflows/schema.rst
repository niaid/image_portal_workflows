************************
Data Flow in a Hedwig
************************

A workflow is a collection of tasks which transforms data through a collection of operations. Each operation is perfomed within a functional unit called `task`, and such tasks are chained together via data passing to form a workflow.

Hedwig workflows maintain a general structure which are a series of tasks chained together.
Each task tentatively looks like:

  - takes a file address as an input,
  - processes it,
  - produces a new output or alters it in place,
  - and returns the file address of output

In this document, we are proposing a generic prototype for how to organize such workflows.

.. literalinclude:: taskio_prototype.py
   :language: python
   :linenos:

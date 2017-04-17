The Pipeline: Feature building, modeling, and evaluation
========================================================

There are three major steps to the pipeline: features building, modeling,
and model evaluation.  Each step is a submodule of the ``pipeline`` and has its
own ``run`` command-line interface, designed to be run from the repository root
as ``python -m pipeline.component.run`` with command-line arguments. Or all
three steps may be run with a single invocation of ``python -m pipeline.run``.
The ``-h`` flag will show the help for each command. A broad overview of each
component is provided here, with more specific inline documentation in the code
and exposed as module documentation below.

Preprocessing: Feature building
-------------------------------

The command ``python -m pipeline.preprocessing.run yamls/default_sample.yaml``
will use the sample experiment configuration to build the required feature
table. The feature tables are timestamped with the time at which the command
was run.

Modeling
--------

The command ``python -m pipeline.modeling.run yamls/default_sample.yaml`` will
use the sample experiment configuration and the most recently created feature
tables in order to train all the models specified in the files at the given
splits.

Evaluation
----------

The command ``python -m pipeline.evaluation.run`` will evaluate all unprocessed
models it finds in the database and compute the metrics found in the default
evaluation configuration file.

Module contents
---------------

.. toctree::

    pipeline.preprocessing
    pipeline.evaluation
    pipeline.modeling

Configuration
=============

A number of configuration files are used to setup database credentials and
specify various names and parameters.


Credentials
-----------

There are several configuration files that contain confidential information
(like credentials) that cannot be committed to the repository. Example files
are provided in the ``config`` directory for both database and s3 credentials.
Simply remove the ``example_`` prefix and populate each appropriately.


Constants
---------

The file ``pipeline/default_profile.yaml`` contains a number of constants that
are used throughout the pipeline codebase, including the paths to the
credential files above as well as specific table and column names. It generally
does not need to be modified for use with Johnson County's data.


Experiment configuration
------------------------

The ``yamls`` folder contains the experimental configuration that gets passed
to the pipeline preprocessing and modeling scripts. Each yaml file within that
folder specifies a very specific experimental configuration. See the
``yamls/default_sample.yaml`` file for an example configuration. There are
several very important categories that are broken out as block comments in that
sample configuration:

- **Type of Experiment** rarely changes; the unit entity is a 'person'.

- **Temporal parameters** specify the time blocking, including start dates, the
  prediction window (in days), and the "fake today" -- this specifies the date
  to simulate the experiment.

- **Labeling details** specify which labels to consider as outcomes. The
  current set of labels are all based upon interactions with a given data
  provider. The label names are underscore-delimited, separating the data
  providers. All but the final component specify the population of interest and
  are currently *exclusive* of any of the other providers (this is something
  that we want to change). The final component specifies the outcome of
  interest. For example: ``jims_mh_jims`` labels the population who has
  previously interacted with criminal justice and mental health (but not EMS)
  who have a new interaction with the criminal justice system as positive
  labels.

- **Feature Selection** specifies the feature groups that should be included in
  the models.

- **Model selection** specifies the models to run and the parameters over which
  they should be parameterized. All model parameters may be lists and get
  combined with the cross-product so all possible combinations get tested.

- **Output file details** specifies where the outputs should go.


Evaluation configuration
------------------------

The variables of interest for evaluation may be specified in the
``pipeline/evaluation/eval_profile.yaml`` file. These determine which
metrics get calculated for each model that was run.

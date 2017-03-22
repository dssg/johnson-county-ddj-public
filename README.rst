Johnson County Early Intervention System
========================================

DSSG has partnered with Johnson County and Salt Lake County to build a
prototype early intervention system (EIS) for individuals who repeatedly
cycle through multiple systems, including jails, EMS, mental health
services. Currently, there is little coordination between systems to
address each person’s underlying needs. An accurate Early Intervention
System will quickly identify individuals at risk of contact with any or
all systems so our partners can provide appropriate services and
interventions to them.

To achieve this goal, we developed models that assign risk scores to
individuals making contact with one system of making future contact with
another system. These models produce ranked lists of individuals at risk
who may receive follow-up care or interventions. The models provide
proactive risk warnings at points of contact (e.g., EMS dispatch, jail
bookings).

Installation
------------

Git clone the following repo (add setup.py file to run when we are
ready)

Setting up the Virtual Environment
----------------------------------

The scripts, notebooks, and other tools in this repository rely on a specific
Python enviroment combining Python 2.7 and a set of package versions specified
in requirements.txt. To ensure that the code runs on your machine, follow the
steps outlined below to set up and activate a Python virtual environment with
the required configuration:

ONE: Ensure that you have Python 2.7 installed on your machine and that you know
the directory where it is installed.

TWO: If you do not have virtual environment installed, install it using:

    $ pip install virtualenv

THREE: Create the virtual environment to use with this software. First change your
working directory to the directory where you would like to install the virtual
environment. Then, create a virtual environment with the following command,
replacing `/usr/bin/python2.7` with the location of your Python 2.7 installation
and `venv` with the name you would like to give to the environment:

    $ virtualenv -p /usr/bin/python2.7 venv

FOUR: Activate the virtual environment, replacing `venv` with the directory
you just created:

    $ source venv/bin/activate

To make activating the virtual environment in the future easier, consider adding
an alias to your .bashrc or .bash_profile:

    alias venv="source /PATH/TO/VIRTUAL/ENVIRONMENT/venv/bin/activate"

FIVE: To configure the virtual environment to use the correct packages and
versions, run the follwing commands, pointing to the requirements.txt file in
the repository for the final one:

    $ pip install numpy==1.11.2
    $ pip install scipy==0.18.1
    $ pip install -r requirements.txt

If this fails, you may need to open requirements.txt and install each package
individually. For example:

    $ pip install collate==0.1.0

SIX: To set up the virtual environment for use within Jupyter Notebooks, run the
following command:

    $ ipython kernel install --user
    Installed kernelspec python2 in /home/USER/.local/share/jupyter/kernels/python2

Copy the kernelspec to a directory where ipython will find it and give it a name
you will recognize as your virtual environment (`venv` in this example):

    $ mkdir -p ~/.ipython/kernels
    $ mv ~/.local/share/jupyter/kernels/python2 ~/.ipython/kernels/venv

Then, edit the `kernel.json` file in the directory you just created, changing
the JSON key called `display_name` to the name of your virtual environment
(e.g., `venv`).

SEVEN: When you are finished working with the tools in this repository, deactivate
your virtual environment with:

    $ deactivate


Setup
-----

To set up an early intervention system for an entity with the
appropriate datasets, you will need to write some configuration files
that define the data sources available and how the code should connect
to the database and what features to create

Database Connection and Data Definition
---------------------------------------

Initial setup is performed via two configuration files, one that
contains database credentials, and one that contains configuration
unique to the public service data

-  Database credentials are stored in a JSON file in the main directory of the
   repository. See ``example_database_profile.json`` for a template.
-  The file ``default_profile.yaml`` in the root directory ``pipeline/``
   contains additional database configuration information, specifying the names
   of tables to use for feature generation and results storage. Use
   ``example_default_profile.yaml`` as a template::

    # this is the path to the database profile containing login information
    db_connection_config_path: "../example_database_profile.json"

    # This path should always point to itself
    DBSETUP: "pipeline/example_default_profile.yaml"

    #########################
    #    Table names        #
    #########################

	# name of id column shared across all dbs for individuals
	# This should be an ssn, hashed ssn, or artificial id assigned for individuals
	id_column: "id_col"

	# This tables depend on which features you want to include
	table1: "schema1.table1"
	table2: "schema1.table2"
	table3: "schema2.table3"

	# this table contains a minimum of three columns:
	# id_column, event, begin_date
	# Note, depending on your features this can contain more features such as end_date
	personid_event_dates: "schema_n:personid_event_date_name"

	# table to save models and predictions in
	models: model_table_name
	predictions: predictions_table_name

	# schema where feature tables will be saved
	feature_schema: feature_table_schema_name

Preprocessing and Running Models
--------------------------------

There are two steps that are needed to run models. Firstly, the
preprocessing module needs to be run. This module produces all the
feature tables for the training and testing data sets for the
appropriate fake todays and prediction windows. Then the models run on
these tables to make predictions.

For the purposes of adjustability all the settings for preprocessing and
modeling are controlled from YAML files in the ``yamls\`` directory. To
view a sample of a yaml file look at ``yamls/default_sample.yaml``.
There are multiple yaml files in this directory because while it is
possible to run different sets of models and labels at the same time
using ``tmux``.

Issues
------

Please use [Github's issue
tracker]<https://github.com/dssg/johnson-county-ddj-public/issues>

Contributors
------------

Matt Bauman, Kate Boxer, Eddie Lin, Hareem Naveed, Erika Salomon, Joe Walsh, Jen
Helsby, Lauren Haynes

To safely and easily access the data from this github repository, I
added data/ to .gitignore and created an alias:

``ln -s /mnt/data/johnson_county/ data``

Requirements
============

- Python 2.7

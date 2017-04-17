Installation
============

Use Git to clone the repository.

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

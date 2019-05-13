# BEERS_UTILS

## Installation using python virtual environments

Somewhere under your home directory, clone the BEERS_UTILS repository:

```bash
git clone git@github.com:itmat/BEERS_UTILS.git
```

At the moment, there is no need to setup BEERS_UTILS in its own virtual environment.
This package provides support to others in the BEERS suite rather than any sort of
functionality by itself. It's probably better to install this package in the virtual
environment you've created for another package in the BEERS suite. To do this, activate
the virtual environment in which you want to install BEERS_UTILS, and then run the
following command:

```bash
pip install -e /path/to/BEERS_UTILS
```

This takes the BEERS_UTILS directory, packages it and creates a link to the packaged
version in <virtualenv>/lib/python3.6/site-packages. This allows python to find the
beers_utils package and subpackages from this virtual environment.

## Funding

Work on CAMPAREE is supported by R21-LM012763-01A1: “The Next Generation of RNA-Seq Simulators for Benchmarking Analyses” (PI: Gregory R. Grant).

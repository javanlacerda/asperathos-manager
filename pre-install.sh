#!/bin/bash

apt-get update
apt-get -y install python-dev
apt-get -y install python-pip
pip install setuptools
pip install tox
pip install flake8

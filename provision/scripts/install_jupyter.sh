#!/bin/bash

# Disable firewall and allow port
sudo ufw Disable
sudo iptables -A INPUT -p tcp --dport 8888 -j ACCEPT

which jupyter || conda install jupyter

cat >> /home/vagrant/.bashrc <<- EOM

# Pyspark Settings
export PYSPARK_PYTHON=/home/vagrant/miniconda/bin/python
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip 0.0.0.0 --notebook-dir=/vagrant"

EOM

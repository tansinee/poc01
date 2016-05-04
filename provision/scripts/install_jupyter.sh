#!/bin/bash
JUPYTER_PORT=8888

`which jupyter` || conda install jupyter

# Disable firewall and allow port
sudo ufw Disable
sudo iptables -A INPUT -p tcp --dport $JUPYTER_PORT -j ACCEPT

pip install --pre toree
sudo /home/vagrant/miniconda/bin/jupyter toree install --user --spark_home=/opt/spark --interpreters=PySpark --python=python

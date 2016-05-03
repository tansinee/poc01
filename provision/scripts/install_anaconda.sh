#!/bin/sh

MINICONDA_URL=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
MINICONDA_SCRIPT=`basename $MINICONDA_URL`
MINICONDA_HOME="/home/vagrant/miniconda"

if [ ! -f "./$MINICONDA_SCRIPT" ] ; then
  wget $MINICONDA_URL
fi

#mkdir -p $MINICONDA_HOME
bash $MINICONDA_SCRIPT -b -p $MINICONDA_HOME
chown -R vagrant:vagrant $MINICONDA_HOME

cat >> /home/vagrant/.bashrc <<- EOM
# Appended by miniconda setup
export PATH="$MINICONDA_HOME/bin:\$PATH"
EOM

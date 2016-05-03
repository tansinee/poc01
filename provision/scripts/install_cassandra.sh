#!/bin/sh

echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y dsc30 cassandra cassandra-tools

#start cassandra service
sudo service cassandra start

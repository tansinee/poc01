#!/bin/sh

echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y dsc30 cassandra cassandra-tools

#config cassandra.yaml
sed -i 's/listen_address: localhost/# listen_address: localhost\nlisten_interface: eth0/' /etc/cassandra/cassandra.yaml
sed -i 's/rpc_address: localhost/# rpc_address: localhost\nrpc_interface: eth0/' /etc/cassandra/cassandra.yaml

#start cassandra service
sudo service cassandra start

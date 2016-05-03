#!/bin/sh

# Perform all operations in temporary directories
cd /tmp

# Load spark package and extract it
if [ ! -d "./spark" ] ; then
  [ ! -e "./spark-1.6.1-bin-hadoop2.6.tgz" ] && wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz
  tar zxf spark-1.6.1-bin-hadoop2.6.tgz
  sudo mv spark-1.6.1-bin-hadoop2.6 spark
fi

# Clean existing spark home
sudo rm -rf /opt/spark
sudo mv -f spark /opt

# Assign SPARK_HOME environment variable
sudo cat > /etc/profile.d/Z11-spark-home-env.sh <<- EOF
#!/bin/sh
export SPARK_HOME=/opt/spark
EOF
sudo chmod +x /etc/profile.d/Z11-spark-home-env.sh

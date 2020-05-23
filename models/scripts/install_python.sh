#!/usr/bin/env bash
echo "Installing and Setting up Python 3.6.5 - started"

echo "Python 3.6.5 download - started"

sudo yum -y update --skip-broken
sudo yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel
readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel
wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz

echo "Python 3.6.5 download - completed"
echo "Python 3.6.5 installation - started"

tar xzf Python-3.6.5.tgz
cd Python-3.6.5
sudo ./configure
sudo make
sudo make install

echo "Python 3.6.5 installation - completed"
echo "Note down the path for python3.6 and pip3 for future reference"

python36_path=`which python3.6`
pip3_path=`which pip3`
echo "$python36_path"
echo "$pip3_path"

echo "Installing sia/models python dependencies - started"

pip3 install certifi==2018.11.29 chardet==3.0.4 Click==7.0 Flask==1.0.2 Flask-Cors==3.0.7 gevent==1.4.0 greenlet==0.4.15 gunicorn==19.9.0 idna==2.8 itsdangerous==1.1.0 Jinja2==2.10 MarkupSafe==1.1.0 numpy==1.15.4 pandas==0.20.3 py4j==0.10.4 python-dateutil==2.7.5 pytz==2018.9 pywebhdfs==0.4.1 requests==2.21.0 scikit-learn==0.20.0 scipy==1.2.0 six==1.12.0 slackclient==1.3.1 urllib3==1.24.1 Werkzeug==0.14.1 cloudpickle==0.5.6 kafka-python==1.4.5

echo "Installing sia/models python dependencies - completed"
echo "Cleaning up directory - started"

rm -rf Python-3.6.5.tgz
rm -rf Python-3.6.5

echo "Cleaning up directory - completed"
echo "Installing and Setting up Python 3.6.5 - completed"

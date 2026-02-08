#!/bin/bash -e

## increase swap file size first..
# https://stackoverflow.com/questions/64821441/collecting-package-metadata-repodata-json-killed
# https://nofence.tistory.com/30sw
# https://repost.aws/knowledge-center/ec2-memory-swap-file

sudo fallocate -l 4G /swapfile 
sudo chmod 600 /swapfile 
sudo mkswap /swapfile 
sudo swapon /swapfile 
sudo cp /etc/fstab /etc/fstab.bak 
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

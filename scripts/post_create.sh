#!/usr/bin/env bash
chmod +x -R scripts/

sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    postgresql-15 \
    postgresql-15-postgis-3 \
    postgresql-15-postgis-3-scripts \
    postgis \
    unzip \
    wget

sudo scripts/libpostal.sh

pip install -r requirements.txt
#!/usr/bin/env bash

source /etc/profile

export PATH=~/miniconda3/bin:$PATH

conda create -n mosquito_monitor python=3.7 -y

source activate mosquito_monitor

pip install --upgrade pip

pip install -r ./requirements.txt

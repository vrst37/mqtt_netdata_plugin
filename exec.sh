#!/usr/bin/env bash

export PATH=~/miniconda3/bin:$PATH

source activate mosquito_monitor

python ./mosquito_monitor.py

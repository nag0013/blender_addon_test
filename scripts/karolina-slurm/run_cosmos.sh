#!/usr/bin/env bash

ml cuDNN/9.7.1.26-CUDA-12.8.0   
ml CUDA/12.8.0

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:=0}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:=./checkpoints}"
export NUM_GPU="${NUM_GPU:=1}"
export HF_HOME=/mnt/proj1/open-35-29/cosmos_hf
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

OUT_DIR=${PWD}/output
CONFIG_DIR=${PWD}/in/config.json

echo ${PWD} > ${PWD}/pwd.txt
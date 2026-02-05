#!/usr/bin/env bash

ml cuDNN/9.7.1.26-CUDA-12.8.0   
ml CUDA/12.8.0

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:=0}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:=./checkpoints}"
export NUM_GPU="${NUM_GPU:=1}"
export HF_HOME=/mnt/proj1/open-35-29/cosmos_hf
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

ROOT_DIR=${PWD}/../
OUT_DIR=${ROOT_DIR}/output
CONFIG_DIR=${ROOT_DIR}/in/config.json

if test -f ${CONFIG_DIR}; then
  echo "File exists." > ${ROOT_DIR}/config_dir.txt
fi

FILE=$1     
if [ -f $FILE ]; then
   echo "File exists." > ${ROOT_DIR}/config_dir.txt
else
   echo "File NOT exists." > ${ROOT_DIR}/config_dir.txt
fi
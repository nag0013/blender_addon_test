#!/usr/bin/env bash

ml cuDNN/9.7.1.26-CUDA-12.8.0   
ml CUDA/12.8.0

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:=0}"
export CHECKPOINT_DIR="${CHECKPOINT_DIR:=./checkpoints}"
export NUM_GPU="${NUM_GPU:=1}"
export HF_HOME=/mnt/proj1/open-35-29/cosmos_hf
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

ROOT_DIR=$1
OUT_DIR=${ROOT_DIR}/outputs
CONFIG=${ROOT_DIR}/in/config.json

if test -f ${CONFIG_DIR}; then
    echo "File exists." > config_dir.txt
else
    echo "File NOT exists." > config_dir.txt
fi

mkdir -p ${OUT_DIR}

PYTHONPATH=$(pwd) torchrun \
    --nproc_per_node=$NUM_GPU \
    --nnodes=1 \
    --node_rank=0 \
    --master_port=29501 \
    cosmos_transfer1/diffusion/inference/transfer.py \
    --checkpoint_dir $CHECKPOINT_DIR \
    --video_save_folder ${OUT_DIR} \
    --controlnet_specs ${CONFIG}\
    --offload_diffusion_transformer \
    --offload_text_encoder_model \
    --offload_guardrail_models \
    --offload_prompt_upsampler \
    --num_gpus $NUM_GPU
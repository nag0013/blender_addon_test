#!/bin/bash

set +e
###############################################
BLEND_FILE=$@
###############################################
if [ ${#work_dir} -ge 1  ]; then
  cd ${work_dir}
  mkdir -p job
  cd job
fi
###############################################
ROOT_DIR=${PWD}/../

LOG_DIR=${ROOT_DIR}/log
IN_DIR=${ROOT_DIR}/in
OUT_DIR=${ROOT_DIR}/out
CACHE_DIR=${ROOT_DIR}/cache

LOG=${LOG_DIR}/${FRAME}.log
ERR=${LOG_DIR}/${FRAME}.err

###############################################

mkdir -p ${LOG_DIR}
mkdir -p ${IN_DIR}
mkdir -p ${OUT_DIR}
mkdir -p ${CACHE_DIR}

###############################################
if [ ${#work_dir} -ge 1  ]; then
  sacct --format=JobID%20,Jobname%50,state,Submit,start,end -j ${depends_on##* } > ${work_dir}.job
fi
###############################################

if [ "$SLURM_ARRAY_TASK_ID" -eq "$SLURM_ARRAY_TASK_MAX" ]; then
  module load FFmpeg
  cd ${OUT_DIR}
  ffmpeg -framerate 24 -i %06d.png -c:v libx264 -pix_fmt yuv420p output.mp4
fi


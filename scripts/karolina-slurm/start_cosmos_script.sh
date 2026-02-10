#!/bin/bash

###############################################
if [ ${#work_dir} -ge 1  ]; then
  cd ${work_dir}
  mkdir -p job
  cd job

  sacct --format=JobID%20,Jobname%50,state,Submit,start,end -j ${SLURM_JOBID} | grep -v "\." > ${work_dir}.job 
fi
###############################################
ROOT_DIR=${PWD}/../

LOG_DIR=${ROOT_DIR}/log
IN_DIR=${ROOT_DIR}/in
OUT_DIR=${ROOT_DIR}/out
CACHE_DIR=${ROOT_DIR}/cache
RUN_COSMOS=${HOME}/blender_addon_test/scripts/karolina-slurm/run_cosmos.sh

LOG=${LOG_DIR}/${FRAME}.log
ERR=${LOG_DIR}/${FRAME}.err
LOG_XORG=${LOG_DIR}/${FRAME}_XORG.log
ERR_XORG=${LOG_DIR}/${FRAME}_XORG.err

###############################################

mkdir -p ${LOG_DIR}
mkdir -p ${IN_DIR}
mkdir -p ${OUT_DIR}
mkdir -p ${CACHE_DIR}

###############################################

. /mnt/proj1/open-35-29/cosmos-on-karolina/pixi_enable.cmd

cd /mnt/proj1/open-35-29/cosmos-on-karolina/

. ${RUN_COSMOS} ${ROOT_DIR}
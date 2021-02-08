#!/bin/bash
#SBATCH --partition=short-serial
#SBATCH --array=1-10
#SBATCH --job-name=cliwoc_ct
#SBATCH --output=slurm_log_output/cliwoc_ct_%A_%a.out
#SBATCH --error=slurm_log_output/cliwoc_ct_%A_%a.err
#SBATCH --mem=4000
#SBATCH --time=00:29:00

source activate ~/miniconda3/envs/sst

echo "starting from $SLURM_ARRAY_TASK_ID"

python ~mdf_reader/tests/gather_stats_c99.py $SLURM_ARRAY_TASK_ID

echo "Done slurm task ID = $SLURM_ARRAY_TASK_ID"

##sacct -j $SLURM_JOB_ID --format=User,JobID,Jobname,partition,state,time,start,end,elapsed,MaxRss,MaxVMSize,nnodes,ncpus,nodelist >> ../slurm_log_output/job_stats.out
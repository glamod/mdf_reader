#!/bin/bash
#SBATCH --partition=short-serial-4hr
#SBATCH --array=31-233
#SBATCH --job-name=cliwoc_ct
#SBATCH --output=slurm_log_output/cliwoc_ct_%A_%a.out
#SBATCH --error=slurm_log_output/cliwoc_ct_%A_%a.err
#SBATCH --mem=4000
#SBATCH --time=00:29:00

module load jaspy

source /home/users/brecinos/c3s_work/env0/bin/activate

echo "starting from $SLURM_ARRAY_TASK_ID"

python /home/users/brecinos/c3s_work/mdf_reader/tests/gather_stats_c99.py $SLURM_ARRAY_TASK_ID

echo "Done slurm task ID = $SLURM_ARRAY_TASK_ID"

##sacct -j $SLURM_JOB_ID --format=User,JobID,Jobname,partition,state,time,start,end,elapsed,MaxRss,MaxVMSize,nnodes,ncpus,nodelist >> ../slurm_log_output/job_stats.out

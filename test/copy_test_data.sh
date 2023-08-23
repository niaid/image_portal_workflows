#!/usr/bin/env bash
#
# This script is a first cut for copying test data from the HPC...
# Using rsync instead of scp in case transfer fails in middle of large file
#  - must have account with read access to the RMLEMHedwigDev and RMLEMHedwigQA shares
#  - must be logged into VPN
#  - cd to top-level "image_portal_workflows" dir of your repo

# test_brt - one 970MB file
mkdir -p test/input_files/brt_inputs
rsync --progress --stats -ave ssh ai-rmlsbatch1.niaid.nih.gov:/mnt/ai-fas12/RMLEMHedwigQA/test/input_files/brt_inputs/Projects/2013-1220-dA30_5-BSC-1_10.mrc test/input_files/brt_inputs/

# test_sem - directory of smallish files, but 1.1GB total
mkdir -p test/input_files/sem_inputs
rsync --progress --stats -ave ssh ai-rmlsbatch1.niaid.nih.gov:/mnt/ai-fas12/RMLEMHedwigDev/Projects/BCBB_TEST/test/input_files/sem_inputs/ test/input_files/sem_inputs/

# test_dm - These were in the repo as of March 2023, but uncomment to copy
#rsync -ave ssh ai-rmlsbatch1.niaid.nih.gov:/mnt/ai-fas12/RMLEMHedwigQA/test/input_files/dm_inputs/Projects/Lab/PI/  test/input_files/dm_inputs/

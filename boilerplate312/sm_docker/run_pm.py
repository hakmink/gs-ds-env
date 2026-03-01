import os
import time
import mimetypes
import json
import shutil
import pickle
import traceback
import argparse
import warnings
import boto3
from botocore.exceptions import ClientError
import papermill as pm
from papermill.exceptions import PapermillExecutionError
import pprint

import run_pm_utils as utils
import conf

pp = pprint.PrettyPrinter(width=41, compact=True, indent=4)
warnings.filterwarnings('ignore')
logs = []


# ----------------------------
# Argument Parsing
# ----------------------------
def parse_args():
    try:
        parser = argparse.ArgumentParser(description="AutoML Experiment Configuration")
        parser.add_argument('--project_hashkey', type=str, default='')
        parser.add_argument('--experiment_hashkey', type=str, default='')
        parser.add_argument('--profile_hashkey', type=str, default='')
        parser.add_argument('--experiment_table_name', type=str, default='')
        parser.add_argument('--experiment_result_table_name', type=str, default='')
        parser.add_argument('--dataset_table_name', type=str, default='')
        parser.add_argument('--dataset_profile_table_name', type=str, default='')
        parser.add_argument('--model_repo_table_name', type=str, default='')
        parser.add_argument('--username', type=str, default='')
        parser.add_argument('--task_token', type=str, default='')
        parser.add_argument('--dryrun', type=str, default='false')
        parser.add_argument('--job_type', type=str, default='')
        args, unknown = parser.parse_known_args()
    
        print('args ++++')
        pp.pprint(vars(args))
        print('unknown ++++')
        pp.pprint(unknown)
        return args
    except Exception as e:
        pp.pprint(e)
        logs.append(str(e))
        pass



# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    try:
        args = parse_args()
        pp.pprint(args)
        
    except Exception as e:
        print(e)
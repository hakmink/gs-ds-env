import os
import time
import json
import shutil
import pickle
import traceback
import argparse
import warnings
import boto3
import papermill as pm
from papermill.exceptions import PapermillExecutionError
import pprint

import run_pm_utils as utils
import conf

pp = pprint.PrettyPrinter(width=41, compact=True, indent=4)
warnings.filterwarnings('ignore')

# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":

    try:
        print('here!!!')
    except Exception as e:
        print(e)
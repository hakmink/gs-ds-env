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
# Utility Functions
# ----------------------------
def prepare_directories(root, job_type):
    try:
        # input
        input_dir = os.path.join(root, job_type, 'input')
        os.makedirs(os.path.join(input_dir, 'profile'), exist_ok=True)
        os.makedirs(os.path.join(input_dir, 'meta'), exist_ok=True)
        os.makedirs(os.path.join(input_dir, 'df'), exist_ok=True)
        os.makedirs(os.path.join(input_dir, 'conf'), exist_ok=True)
        os.makedirs(os.path.join(input_dir, 'model'), exist_ok=True)
        # output
        artifacts_dir = os.path.join(root, job_type, 'artifacts')
        os.makedirs(artifacts_dir, exist_ok=True)
        return input_dir, artifacts_dir
    except Exception as e:
        pp.pprint(e)
        logs.append(str(e))
        pass


def fetch_metadata_and_log(conf, args):
    try:
        exp = utils.get_experiment_item(args.experiment_table_name, args.project_hashkey, args.experiment_hashkey)
        if exp is not None and 'file_hashkey' in exp:
            dataset = utils.get_dataset_item(args.dataset_table_name, args.project_hashkey, exp['file_hashkey'])
        else:
            dataset = None
        if 'profile_hashkey' in args:
            profile = utils.get_dataset_item(args.dataset_profile_table_name, args.project_hashkey, args.profile_hashkey)
        else:
            profile = None
        if  exp is not None and 'model_artifact_hashkey' in exp and 'model_type' in exp:
            table_name = 'automl-model-type-experiment-result'.replace('model-type',exp['model_type'])
            model_artifact = utils.get_experiment_item(table_name, args.project_hashkey, exp['model_artifact_hashkey'])
        else:
            model_artifact = None
        if  exp is not None and 'model_hashkey' in exp:
            model = utils.get_model_repo_item(args.model_repo_table_name, exp['model_hashkey'])
        else:
            model = None
    
        ts = int(time.time())
        log_item = {
            'target': args.experiment_table_name,
            'job_type': args.job_type,
            'created_ts': ts,
            'created_dt': utils.conv_ts_to_dt_str(ts),
            'project_hashkey': args.project_hashkey,
            'experiment_hashkey': args.experiment_hashkey,
            'file_hashkey': exp['file_hashkey'],
            'experiment': exp,
            'dataset': dataset,
            'model': model,
            'username': args.username
        }
    
        utils.put_item_to_ddb(conf.log_table_name, log_item)
        return exp, dataset, profile, model, model_artifact, log_item
    except Exception as e:
        pp.pprint(e)
        logs.append(str(e))
        pass


def download_resources(exp, dataset, profile, model, model_artifact, input_dir, root):
    try:
        print('download_resources+++')
        print('++ model ++++++')
        pp.pprint(model)
        print('++ dataset ++++++')
        pp.pprint(dataset)
        print('++ profile ++++++')
        pp.pprint(profile)
        # model
        if model is not None:
            utils.download_s3_files_to_directory(model['bucket_name'], 
                                                 model['s3_zip_key_path'], 
                                                 root)
        # dataset
        if dataset is not None:
            for key in ['s3_key_sample_df_file', 's3_key_column_info_file']:
                utils.download_s3_file_to_directory(dataset['bucket_name'], 
                                                    dataset[key], 
                                                    os.path.join(input_dir, 'meta'))
            utils.download_s3_files_to_directory(dataset['bucket_name'], 
                                                 dataset['s3_key_df_path'], 
                                                 os.path.join(input_dir, 'df'))
        # modeling config
        config_key = f"{exp['s3_key_prefix']}/config.yml"
        if exp is not None:
            utils.download_s3_file_to_directory(exp['bucket_name'], 
                                                config_key, 
                                                os.path.join(input_dir, 'conf'))
        # dataset profile
        if profile is not None and 'artifacts' in profile:
            for prefix, files in profile['artifacts'].items():
                cleaned_prefix = prefix.rstrip('./')  # '.'이나 '/'로 끝나는 경우 제거
                for file in files:
                    utils.download_s3_file_to_directory(profile['bucket_name'], 
                                                        f"{cleaned_prefix}/{file}", 
                                                        os.path.join(input_dir, 'profile'))
        # model artifact
        if model_artifact is not None and 'artifacts' in model_artifact:
            for prefix, files in model_artifact['artifacts'].items():
                if 'artifacts/model' in prefix:
                    cleaned_prefix = prefix.rstrip('./')  # '.'이나 '/'로 끝나는 경우 제거
                    for file in files:
                        utils.download_s3_file_to_directory(model_artifact['bucket_name'], 
                                                            f"{cleaned_prefix}/{file}", 
                                                            os.path.join(input_dir, 'model'))
        
    except Exception as e:
        pp.pprint(e)
        logs.append(str(e))
        pass
        

def run_papermill(input_nb, output_dir):
    os.chdir(output_dir)
    output_nb = input_nb.replace('.ipynb', '_output.ipynb')
    try:
        pm.execute_notebook(
            input_nb,
            os.path.join('./artifacts', output_nb),
            parameters=dict(),
            kernel_name=conf.kernel_name,
            report_mode=True
        )
    except PapermillExecutionError as e:
        pp.pprint(e)
        logs.append(str(e))
        pass


def finalize_and_upload(conf, args, exp, artifacts_dir, sfn_client, start_ts):
    try:
        bucket = exp['bucket_name']
        s3_prefix = f"{exp['s3_key_prefix']}/artifacts"
        artifacts = utils.upload_directory_to_s3("artifacts", bucket, s3_prefix)

        experiment_done = (
            (f'{s3_prefix}/model' in artifacts and 'model.pkl' in artifacts[f'{s3_prefix}/model']) or
            (f'{s3_prefix}/df' in artifacts and 'inferred_df_part0.parquet' in artifacts[f'{s3_prefix}/df'])
        )
    
        exp['status'] = '실험 완료' if experiment_done else '실험 실패'
        ts = int(time.time())
        exp['updated_dt'] = utils.conv_ts_to_dt_str(ts)
        exp['updated_ts'] = ts
        utils.put_item_to_ddb(args.experiment_table_name, exp)
    
        res_item = {
            'artifacts': artifacts,
            'bucket_name': exp['bucket_name'],
            's3_key_prefix': exp['s3_key_prefix'],
            'created_ts': ts,
            'created_dt': utils.conv_ts_to_dt_str(ts),
            'dataset_name': exp['dataset_name'],
            'file_hashkey': exp['file_hashkey'],
            'model_hashkey': exp['model_hashkey'],
            'model_name': exp['model_name'],
            'project_hashkey': exp['project_hashkey'],
            'experiment_hashkey': exp['experiment_hashkey'],
            'project_name': exp['project_name'],
            'username': exp['username'],
            'elapsed': ts - start_ts,
            'experiment_done': experiment_done,
            'logs': logs
        }
    
        pp.pprint(res_item)
        utils.put_item_to_ddb(args.experiment_result_table_name, res_item)
    
        sfn_client.send_task_success(
            taskToken=args.task_token,
            output=json.dumps({
                'project_hashkey': args.project_hashkey,
                'experiment_hashkey': args.experiment_hashkey,
                'file_hashkey': args.experiment_hashkey,
                'experiment_table_name': args.experiment_table_name,
                'experiment_result_table_name': args.experiment_result_table_name,
                'dataset_table_name': args.dataset_table_name,
                'dataset_profile_table_name': args.dataset_profile_table_name,
                'model_repo_table_name': args.model_repo_table_name,
                'username': args.username,
                'statusCode': 200,
                'body': exp['status'],
                'experiment_done': experiment_done,
                'logs': logs
            })
        )
    except Exception as e:
        pp.pprint(e)
        logs.append(str(e))
        pass


def handle_error(sfn_client, token, error):
    print(error)
    error_message = str(traceback.format_exc())
    sfn_client.send_task_failure(
        taskToken=token,
        error='실험 실패',
        cause=error_message,
    )


# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    args = parse_args()
    conf_info = conf.get_info()
    pp.pprint(conf_info)

    sfn_client = boto3.client('stepfunctions', region_name=conf_info['region_name'])
    start_ts = int(time.time())

    try:
        # step 1
        input_dir, artifacts_dir = prepare_directories('./work', args.job_type)
        exp, dataset, profile, model, model_artifact, log_item = fetch_metadata_and_log(conf, args)

        with open(os.path.join(input_dir, 'log_item.pkl'), 'wb') as f:
            pickle.dump(log_item, f)

        # step 2
        download_resources(exp, dataset, profile, model, model_artifact, input_dir, './work')
        utils.print_tree('.')

        # step 3
        if args.dryrun.lower() == 'true':
            print('dryrun mode: skipping papermill execution')
        else:
            run_papermill(model[f"{args.job_type}_ipynb"], f"./work/{args.job_type}")
        utils.print_tree('.')
        
        # step 4
        finalize_and_upload(conf, args, exp, artifacts_dir, sfn_client, start_ts)

    except Exception as e:
        handle_error(sfn_client, args.task_token, e)

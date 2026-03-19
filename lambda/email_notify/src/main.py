import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

import requests
import json

import pprint
pp = pprint.PrettyPrinter(indent=4)

import os
region_name = os.getenv("REGION_NAME")


def get_parameter_key(param_name='/sean-credential/smtp2go-key'):
    """
    Systems Manager Parameter Store 에서 SecureString 타입의 파라미터를 가져옵니다.
    """
    try:
        # ssm = boto3.client('ssm', region_name=region_name)
        # response = ssm.get_parameter(
        #     Name=param_name,
        #     WithDecryption=True  # SecureString일 경우 True 필수
        # )
        # return response['Parameter']['Value']
        return "app-3Vhzd1RYHuijiVRMF2z15EMo"
    except ClientError as e:
        raise RuntimeError(f"Parameter Store 접근 중 오류 발생: {e}")


def send_email(recipient_email, subject, body, from_email='GS Microwave 팀 <sean@52g.team>'):
    
    try:

        url = 'https://api.smtp2go.com/v3/email/send'
        data = {
          "api_key": get_parameter_key('/sean-credential/smtp2go-key'),
          "to": [
            f"{recipient_email} <{recipient_email}>"
          ],
          "sender": from_email,
          "subject": subject,
          "text_body": body,
          "custom_headers": [
            {
              "header": "Reply-To",
              "value": from_email
            }
          ]
        }
        print(data)
        json_data = json.dumps(data)


        # Content-Type을 application/json으로 설정
        headers = {'Content-Type': 'application/json'}

        response = requests.post(url, data=json_data, headers=headers)

        result = json.loads(response.text)
        print(result)
        
        if result['data']['succeeded'] == 1:
            return True
        else:
            return False
    except Exception as e:
        raise e        
        
        
def lambda_handler(event, context):
    try:
        username = event.get('username','')
        title = event.get('title','')
        email_body = event.get('email_body','')
        from_email = event.get('from_email','GS Microwave 팀 <sean@52g.team>')
        pp.pprint(event)
        
        if (username=='' or title=='' or email_body==''):
            return {
                'statusCode': 500,
                'error': 'validation failed',
                'username': username,
                'title': title,
                'email_body': email_body,
                'error': '발송 데이터가 제대로 들어오지 않았어요'
            }      

        status = send_email(username, title, email_body, from_email)
        
        if status:
            return {
                'statusCode': 200,
                'msg': 'mail sent successfullys',
                'username': username,
                'title': title,
                'email_body': email_body,
            }  
        else:
            return {
                'statusCode': 500,
                'error': 'validation failed',
                'username': username,
                'title': title,
                'email_body': email_body,
                'error': '이메일 발송에 문제가 있어요'
            }  
    except Exception as e:
        print(e)
        msg = '이메일 발송에 문제가 생겼어요: ' + str(e)
        return {
            'statusCode': 500,
            'error': 'validation failed',
            'username': username,
            'title': title,
            'email_body': email_body,
            'error': msg
        }
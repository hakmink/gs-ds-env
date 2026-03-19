# 설치 방법
* 우선,
    * ./zip_requests_layer.sh 실행해서 layer 를 등록
* ./build.sh create 를 해서 lambda 함수 등록
    * role_name="RoleStack-automlcdkprodLambdaExecutionRole8266BD50-TBZP28ImAIyV"
    * role 이름에 맞게 수정
* 실행 결과 예시
```
(streamlit312) [ec2-user@ip-172-16-111-209 email_notify]$ ./build.sh create
create
updating: main.py (deflated 66%)
fetching latest version of layer arn...
latest layer arn: arn:aws:lambda:us-east-1:703671896240:layer:python-requests-layer:1
creating lambda function...
{
    "FunctionName": "email-notify",
    "FunctionArn": "arn:aws:lambda:us-east-1:703671896240:function:email-notify",
    "Runtime": "python3.12",
    "Role": "arn:aws:iam::703671896240:role/RoleStack-automlcdkprodLambdaExecutionRole8266BD50-TBZP28ImAIyV",
    "Handler": "main.lambda_handler",
    "CodeSize": 1333,
    "Description": "",
    "Timeout": 120,
    "MemorySize": 128,
    "LastModified": "2024-08-30T07:12:26.435+0000",
    "CodeSha256": "8dnGSYP5EqNXJWVw8iBGo8ANtFAhbnFFfc+cvH8UUiM=",
    "Version": "$LATEST",
    "TracingConfig": {
        "Mode": "PassThrough"
    },
    "RevisionId": "8906bb5c-3623-4652-8541-3014a25397c2",
    "Layers": [
        {
            "Arn": "arn:aws:lambda:us-east-1:703671896240:layer:python-requests-layer:1",
            "CodeSize": 1029314
        }
    ],
    "State": "Pending",
    "StateReason": "The function is being created.",
    "StateReasonCode": "Creating",
    "PackageType": "Zip",
    "Architectures": [
        "x86_64"
    ],
    "EphemeralStorage": {
        "Size": 512
    },
    "SnapStart": {
        "ApplyOn": "None",
        "OptimizationStatus": "Off"
    },
    "RuntimeVersionConfig": {
        "RuntimeVersionArn": "arn:aws:lambda:us-east-1::runtime:acd6500d0e3f6a085fb07933e3472ed6e58360d19ec5dd91bc7c7e8ad119de42"
    },
    "LoggingConfig": {
        "LogFormat": "Text",
        "LogGroup": "/aws/lambda/email-notify"
    }
}
lambda function created successfully.
```
import boto3
import argparse


####################################################################
def delete_untagged_images(repository_name, region_name):
    try:
        # Boto3 ECR 클라이언트 초기화
        ecr_client = boto3.client('ecr', region_name=region_name)

        # 이미지 목록을 조회
        response = ecr_client.list_images(
            repositoryName=repository_name,
            filter={'tagStatus': 'UNTAGGED'}
        )

        image_ids = response['imageIds']

        # 이미지 ID가 있는 경우, 해당 이미지 삭제
        if image_ids:
            delete_response = ecr_client.batch_delete_image(
                repositoryName=repository_name,
                imageIds=image_ids
            )
            print(f"Deleted images: {delete_response['imageIds']}")
        else:
            print("No untagged images to delete.")

    except Exception as e:
        print(f"Error deleting images: {e}")


####################################################################
if __name__ == '__main__':
    # argparse를 사용하여 명령줄 인수 처리
    parser = argparse.ArgumentParser(description='Delete untagged images from an ECR repository.')
    parser.add_argument('--repository_name', type=str, help='The name of the ECR repository')
    parser.add_argument('--region', type=str, default='us-west-2', help='AWS region (default: us-west-2)')
    args = parser.parse_args()

    # 함수 호출
    if args.repository_name is None:
        print('repository_name 은 꼭 넣어주셔야 해요!')
    else:
        delete_untagged_images(args.repository_name, args.region)

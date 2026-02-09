# 사용 방법
- job_type
    - training
    - inference
- job_type 이 training 이던 inference 던 간에, 테스트하려는 모델 노트북을 최대한 경량으로 만들어서 frame 만 잘 동작하는지 체크하는 것이 중요하다
# 코드 설명
- subprocess 를 이용해서, run_pm.py 를 구동하는 테스트를 해본다
- 디렉토리 구조에 따라, 잘 동작하는지 체크해야 한다
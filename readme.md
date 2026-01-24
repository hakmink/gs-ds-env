# GS DS Environment Manager
> SageMaker AI Notebook 환경에서  
> **uv 기반 가상환경 생성 + Jupyter 커널 등록 + Docker/ECR 배포**를  
> 하나의 표준 프로세스로 관리하기 위한 환경 스캐폴딩

---

## 1. 목적 (Why this exists)

SageMaker AI Notebook 환경에서 다음 문제가 반복적으로 발생한다:

- base/conda 환경 오염
- 패키지 충돌 및 디스크 부족(`/`)
- 커널과 실제 실행 환경 불일치
- 노트북 환경과 Docker/ECR 환경 간 괴리

본 프로젝트는 이를 해결하기 위해:

- **uv + venv 기반 환경 격리**
- **테마(theme) 단위 환경 관리**
- **커널 등록 자동화**
- **동일 requirements로 Docker/ECR 배포**

를 하나의 표준 구조로 제공한다.

---

## 2. 기본 개념

### 2.1 Working Directory

```bash
WORKING_DIR="/home/ec2-user/SageMaker/.myenv"
````

* 모든 가상환경, 캐시, 커널 설정을 `/home` 하위에 둠
* `/` 디스크 압박 및 SageMaker 기본 이미지 오염 방지

---

### 2.2 테마(Theme) 개념

* 테마 = **업무/모델/도메인 단위 환경**
* 예:

  * `tsai` : 시계열 / PatchTST
  * `forecasting` : 일반 예측
  * `cv` : 컴퓨터 비전

각 테마는:

* Jupyter 커널
* Python 가상환경
* Docker 이미지

를 **1:1로 대응**시킨다.

---

## 3. 디렉토리 구조

```text
$WORKING_DIR/
└─ gs-ds-env/
   ├─ bin/
   │  ├─ gs-env-create-kernel.sh      # venv 생성 + 커널 등록
   │  └─ gs-env-docker-build-push.sh  # Docker 빌드 + ECR 푸시
   │
   ├─ tsai/
   │  ├─ kernel/
   │  │  ├─ requirements.txt          # Jupyter/venv용 패키지 정의
   │  │  ├─ manifest.env              # 생성 결과 메타데이터
   │  │  └─ .venv/                    # uv venv (자동 생성)
   │  └─ docker/
   │     ├─ Dockerfile
   │     └─ requirements.txt          # 컨테이너용 패키지 정의
   │
   ├─ tabular/
   │  ├─ kernel/
   │  └─ docker/
   │
   └─ streamlit/
      ├─ kernel/
      └─ docker/
```

---

## 4. 커널 네이밍 규칙

```text
<테마명>_<파이썬버전>
```

예:

* `tsai_312`
* `tabular_310`

> 파이썬 버전은 `3.12 → 312`, `3.10 → 310` 방식으로 표기

---

## 5. 사전 요구사항

* SageMaker AI Notebook (Amazon Linux 2 / 2023)
  * 새 노트북 인스턴스 생성 시 플랫폼을 notebook-al2023-v1 로 선택
* Python ≥ 3.9
* 인터넷 접근 가능
* (Docker/ECR 사용 시)

  * Docker
  * AWS CLI
  * ECR 권한

---

## 6. 가상환경 생성 + 커널 등록

### 6.1 기본 사용법

```bash
export WORKING_DIR="/home/ec2-user/SageMaker/.myenv"

$WORKING_DIR/gs-ds-env/bin/gs-env-create-kernel.sh \
  <theme> <python_version>
```

### 6.2 예시 (tsai + Python 3.12)

```bash
$WORKING_DIR/gs-ds-env/bin/gs-env-create-kernel.sh tsai 3.12
```

결과:

* venv 생성

  ```
  gs-ds-env/tsai/kernel/.venv
  ```
* 커널 등록

  ```
  tsai_312
  ```

JupyterLab에서:

```
Kernel → Change Kernel → tsai_312
```

---

### 6.3 requirements 지정

```bash
$WORKING_DIR/gs-ds-env/bin/gs-env-create-kernel.sh \
  tsai 3.12 \
  $WORKING_DIR/gs-ds-env/tsai/kernel/requirements.txt
```

---

### 6.4 Torch 설치 옵션

기본값:

* GPU 환경 → `cu121`

옵션 지정:

```bash
gs-env-create-kernel.sh tsai 3.12 "" cpu
```

---

## 7. Docker 이미지 빌드 & ECR 배포

### 7.1 Dockerfile 위치

```text
gs-ds-env/<theme>/docker/
```

* `requirements.txt` 는 kernel과 **의도적으로 분리**
* 노트북 ≠ 서빙 환경 차이를 명확히 관리

---

### 7.2 빌드 & 푸시

```bash
$WORKING_DIR/gs-ds-env/bin/gs-env-docker-build-push.sh \
  tsai \
  <aws_account_id> \
  <region> \
  <ecr_repo_name> \
  tsai_312
```

예:

```bash
gs-env-docker-build-push.sh tsai 123456789012 ap-northeast-2 gs-ds-env tsai_312
```

---

## 8. 운영 가이드 (중요)

### 8.1 base/conda 환경 사용 금지

* 모든 작업은 **테마 커널에서만 수행**
* base는 오직 런처 역할

---

### 8.2 디스크 안정성

* pip / uv 캐시:

  ```text
  $WORKING_DIR/.pip-cache
  $WORKING_DIR/.uv-cache
  ```
* `/` 디스크 부족 이슈 재발 방지

---

### 8.3 커널 재등록

```bash
jupyter kernelspec uninstall tsai_312 -f
```

후 재실행.

---

## 9. 철학 (Design Principles)

* 환경은 **사람이 아니라 스크립트가 만든다**
* 노트북은 실험 공간, Docker는 실행 공간
* 커널 이름만 보고도:

  * 무엇을 위한 환경인지
  * 어떤 Python인지
    를 알 수 있어야 한다
* “다음 사람이 와도 그대로 재현 가능”해야 한다

---

## 10. 확장 아이디어 (Roadmap)

* uv lock 파일 관리 (`uv pip compile`)
* GPU 자동 감지 (`cu118 / cu121 / cpu`)
* 테마별 정책 템플릿 (DS / DE / MLOps)
* SageMaker Image / Studio 연계

---

## 11. TL;DR

```bash
# 1. 테마별 환경 + 커널 생성
gs-env-create-kernel.sh tsai 3.12

# 2. Jupyter에서 커널 선택
tsai_312

# 3. 동일 환경으로 Docker/ECR 배포
gs-env-docker-build-push.sh tsai <acct> <region> gs-ds-env tsai_312
```

> **하나의 테마 = 하나의 환경 = 하나의 커널 = 하나의 Docker 이미지**

이 구조를 지키는 것이 이 레포의 전제이다.

---

### 기타
- glibc 버젼 확인 방법
```
ldd --version
# or
/lib64/libc.so.6
# or
rpm -q glibc
```
- 파이썬에서
```
import platform
print(platform.libc_ver())
```
- 바이너리 요구 GLIBC 버전 확인 (디버깅용)
```
strings /path/to/binary | grep GLIBC_ | sort -u
```
- pip / conda 캐시 정리
```
pip cache purge
conda clean -a -y
```
- pip 캐시를 /home 으로 강제 이동 (핵심)
```
export PIP_CACHE_DIR=/home/ec2-user/SageMaker/.pip-cache
mkdir -p $PIP_CACHE_DIR
```
- option
```
pip install tsai --no-cache-dir
pip install tsai --no-deps
```
- uv install
```
uv pip install tsai
```
- kernel
```
uv pip install ipykernel
python -m ipykernel install --user \
  --name tsai-uv \
  --display-name "Python (tsai · uv)"
```
- 연습
```
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

cd /home/ec2-user/SageMaker
mkdir tsai-playground && cd tsai-playground

uv venv .venv
source .venv/bin/activate

export PIP_CACHE_DIR=/home/ec2-user/SageMaker/.pip-cache
uv pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

uv pip install ipykernel
python -m ipykernel install --user --name tsai-uv --display-name "Python (tsai · uv)"

jupyter kernelspec list
jupyter kernelspec uninstall tsai-uv -f
```
#/bin/bash

##################################################
# locale
sudo localedef -f UTF-8 -i ko_KR ko_KR.UTF-8
echo 'LANG=ko_KR.UTF-8' | sudo tee /etc/locale.conf
source /etc/locale.conf
locale

# git
git config --global credential.helper 'cache --timeout=3600'
git config --global credential.helper store
git config --global user.name "hyunju-song"
git config --global user.email hun3780@gmail.com

cp ~/SageMaker/.git-credentials ~/.

# yum
sudo yum install -y htop tree telnet

##################################################
# swap size
cd ~/SageMaker/gs-ds-env/bin/
./increase_swap_size.sh

##################################################
# 환경 시작
echo "alias l='ls -al'" >> ~/.bashrc

# ──────────────────────────────────────────────
# [선택] 커스텀 단축 alias 오버라이드
# 지정하지 않은 환경은 환경 이름 전체가 alias로 자동 등록됨
# ──────────────────────────────────────────────
declare -A CUSTOM_ALIASES=(
    # ["단축명"]="환경이름"
    # ["312"]="tabular312"
    # ["bo"]="boilerplate312"
)

# ──────────────────────────────────────────────
# conda env 자동 감지 (base 및 주석 제외)
# ──────────────────────────────────────────────
CONDA_ENVS_DIR="/home/ec2-user/SageMaker/.myenv/miniconda/envs"

CONDA_ENVS=$(find "$CONDA_ENVS_DIR" -mindepth 1 -maxdepth 1 -type d \
    | xargs -I{} basename {} \
    | sort)

if [[ -z "$CONDA_ENVS" ]]; then
    echo "[ERROR] conda 환경을 찾을 수 없습니다. conda가 초기화되어 있는지 확인하세요."
    exit 1
fi

echo "[INFO] 감지된 conda 환경 목록:"
echo "$CONDA_ENVS" | while read -r env; do echo "  - $env"; done

# ──────────────────────────────────────────────
# 역방향 맵 생성: 환경이름 → 단축 alias
# (커스텀 alias에 없으면 환경 이름 자체를 alias로 사용)
# ──────────────────────────────────────────────
declare -A ENV_TO_ALIAS

# 커스텀 alias를 역방향으로 등록
for short in "${!CUSTOM_ALIASES[@]}"; do
    env_name="${CUSTOM_ALIASES[$short]}"
    ENV_TO_ALIAS["$env_name"]="$short"
done

# ──────────────────────────────────────────────
# 각 환경에 대해 alias 등록 + start_env.sh 실행
# ──────────────────────────────────────────────
while IFS= read -r env_name; do
    [[ -z "$env_name" ]] && continue

    # 커스텀 alias가 있으면 사용, 없으면 환경 이름 전체 사용
    alias_name="${ENV_TO_ALIAS[$env_name]:-$env_name}"

    echo "[INFO] alias 등록: ${alias_name} → conda activate ${env_name}"
    echo "alias ${alias_name}='conda activate ${env_name}'" >> ~/.bashrc

    if [[ -f "./start_env.sh" ]]; then
        echo "[INFO] start_env.sh 실행: ${env_name}"
        ./start_env.sh "${env_name}"
    else
        echo "[WARN] start_env.sh 를 찾을 수 없습니다. 건너뜁니다."
    fi

done <<< "$CONDA_ENVS"

echo "[DONE] 모든 환경 처리 완료. 변경사항 적용: source ~/.bashrc"


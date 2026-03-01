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

# alias 등록
declare -A aliases=(
    ["st"]="streamlit314"
    ["312"]="tabular312"
    ["311"]="lightgbm311"
    ["bo"]="boilerplate312"
)

for alias_name in "${!aliases[@]}"; do
    echo "alias ${alias_name}='conda activate ${aliases[$alias_name]}'" >> ~/.bashrc
done


for alias_name in "${!aliases[@]}"; do
    ./start_env.sh "${aliases[$alias_name]}"
done


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
git config --global user.name "Sean"
git config --global user.email hakmink@gmail.com

cp ~/SageMaker/.git-credentials ~/.

# yum
sudo yum install -y htop tree telnet


##################################################
# # alias
echo "alias l='ls -al'" >> ~/.bashrc
# echo "alias st='conda activate streamlit312'" >> ~/.bashrc
# echo "alias 310='conda activate tabular310_langchain'" >> ~/.bashrc
# echo "alias 312='conda activate tabular312_langchain'" >> ~/.bashrc
# source ~/.bashrc

# # python kernel
# cd ~/SageMaker/gs-tabular3x/notebook_kernel/
# ./increase_swap_size.sh
# ./start_env.sh -f ../streamlit312/notebook_kernel/environment.yml
# ./start_env.sh -f ../tabular310_langchain/notebook_kernel/environment.yml
# ./start_env.sh -f ../tabular312_langchain/notebook_kernel/environment.yml
# clear

# # streamlit run
# cp ~/SageMaker/run.sh ~/run.sh
# clear

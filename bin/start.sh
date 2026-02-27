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
# # alias
echo "alias l='ls -al'" >> ~/.bashrc
echo "alias st='conda activate streamlit314'" >> ~/.bashrc
echo "alias 312='conda activate tabular312'" >> ~/.bashrc
echo "alias 311='conda activate lightgbm311'" >> ~/.bashrc
# echo "alias 312='conda activate tabular312_langchain'" >> ~/.bashrc
# source ~/.bashrc

# # python kernel
cd ~/SageMaker/gs-ds-env/bin/
./increase_swap_size.sh
./start_env.sh streamlit314
./start_env.sh tabular312
./start_env.sh lightgbm311
# clear

# # streamlit run
# cp ~/SageMaker/run.sh ~/run.sh
# clear

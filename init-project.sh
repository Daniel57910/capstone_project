python3 -m venv capstone-venv
source capstone-venv/bin/activate
pip install -r requirements.txt
sudo chmod 755 /mnt1/namenode
echo "EXPORT AIRFLOW_HOME=/home/hadoop" ~/.bashrc
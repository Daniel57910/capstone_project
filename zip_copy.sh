
zip -r project.zip *.py queries/*.py spark/* dags/*.py requirements.txt unzip.sh init-project.sh
scp -i ~/Documents/GitHub/capstone_project/sparkify-lake-2.pem project.zip hadoop@ec2-34-205-37-40.compute-1.amazonaws.com:~
rm project.zip

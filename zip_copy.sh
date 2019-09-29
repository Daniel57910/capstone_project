
zip -r project.zip *.py queries/*.py spark/* dags/*.py requirements.txt unzip.sh init-project.sh
scp -i ~/Documents/GitHub/capstone_project/sparkify-lake-2.pem project.zip hadoop@ec2-18-209-211-78.compute-1.amazonaws.com:~
rm project.zip

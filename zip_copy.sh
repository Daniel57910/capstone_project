rm project.zip
zip -r project.zip queries/* spark/* dags/* requirements.txt unzip.sh init-project.sh
scp -i ~/Documents/GitHub/capstone_project/sparkify-lake-2.pem project.zip hadoop@ec2-3-82-64-38.compute-1.amazonaws.com:~

zip -r project.zip *.py lib/*.py requirements.txt make_venv.sh
scp -i sparkify-lake-2.pem project.zip hadoop@ec2-23-22-150-3.compute-1.amazonaws.com:~
rm project.zip

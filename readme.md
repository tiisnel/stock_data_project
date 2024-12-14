Start of project: saves raw stock data to minio as .csv files
airflow runs daily, but checks last save date, so if run first time, or docker has been closed, downloads longer period.

to run:
create .env based on .env.template
chmod +x composer.sh
composer.sh up

cd stremlit
docker-compose -f docker-compose.streamlit.yml up --build
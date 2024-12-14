This project collects data from yahoo finance stock market and from world bank data. It allows the user to see, how well are both datasets correlated on same timeframe

to run:
create .env based on .env.template NB! if on windows, make sure that lines end with LF, not CRLF

chmod +x composer.sh
./composer.sh init
./composer.sh up

As airflow runs only once a day, first time you should manually trigger the dag from airflow localhost:8080

to view results:
cd streamlit
docker-compose -f docker-compose.streamlit.yml up --build
you can see the results from localhost:8501

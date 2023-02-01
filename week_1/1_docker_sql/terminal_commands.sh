
docker run hello-world

docker build -t test:pandas .

docker run -it test:pandas

docker network create pg-network
docker network ls
docker network rm pg-network


wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

cd /Users/alex/Documents/DEZoomCamp2023/1_week/1_docker_sql

# run local interface to query DB
pgcli -h localhost -p 5432 -u root -d ny_taxi
# list tables: \dt



# jupyter commands
jupyter notebook

jupyter nbconvert --to=script loading_data.ipynb


docker build -t taxi_ingest:v001 .

docker run -it \
  --network=pg_network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"




docker-compose up -d
docker-compose down

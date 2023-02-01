
## (RUN VENV)


```
cd Documents/DEZoomCamp2023/ 
```


```
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
pip3 install -r requirements.txt 
pip3 list
deactivate venv
```

Install certificates if there is error running prefect:
```
/Applications/Python\ 3.7/Install\ Certificates.command
```


## (Start local server)
```
prefect orion start
```
Open UI in:

http://127.0.0.1:4200/api



## (blocks)

```
prefect block register -m prefect_gcp
```

http://127.0.0.1:4200/blocks

* GCP: add service account:
    ** generate JSON key
    ** create GCP Credentials / zoom-gcp-creds
* GCS Bucket / zoom-gcs



```
prefect deployment build task3_parameterized_flow.py:main_flow -n "Parameterized ETL"
```
amend parameters in YAML file 
```
prefect deployment apply main_flow-deployment.yaml
```

to allow deployment run we need to create an agent: 
```
prefect agent start --work-queue "default"
```





## TASK4 
```
prefect deployment build task4_parameterized_flow.py:main_flow -n etl2 --cron "0 0 * * *" -a
```

Docker:

docker image build -t almazini/prefect:zoom .


docker login


docker image push almazini/prefect:zoom


python docker_deployment.py 

prefect deployment run main_flow/docker-flow -p "months=[1,2]"


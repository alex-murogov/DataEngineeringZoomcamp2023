
## (RUN VENV)

```
python3 -m venv week2
source week2/bin/activate
pip3 install -r requirements.txt 
pip3 list
deactivate week2
```

Install certificates if there is error running prefect:
`/Applications/Python\ 3.7/Install\ Certificates.command`


## (Start local server)
```
prefect orion start
```
Open UI in:

http://127.0.0.1:4200/api



## (blocks)

`prefect block register -m prefect_gcp`

http://127.0.0.1:4200/blocks

* GCP: add service account:
    ** generate JSON key
    ** create GCP Credentials / zoom-gcp-creds
* GCS Bucket / zoom-gcs



```
prefect deployment build ./3_parameterized_flow.py:main_flow -n "Parameterized ETL"
```
amend parameters in YAML file 
```
prefect deployment apply main_flow-deployment.yaml
```

to allow deployment run we need to create an agent: 
```
prefect agent start --work-queue "default"
```




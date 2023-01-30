
[//]: # (RUN VENV)
python3 -m venv week2
source week2/bin/activate
pip3 install -r requirements.txt 
pip3 list
deactivate week2

/Applications/Python\ 3.7/Install\ Certificates.command

[//]: # (Start local server)
prefect orion start
http://127.0.0.1:4200/api


[//]: # (blocks)
prefect block register -m prefect_gcp
http://127.0.0.1:4200/blocks
    - GCP: add service account:
        - generate JSON key
    - create GCP Credentials / zoom-gcp-creds
    - GCS Bucket / zoom-gcs



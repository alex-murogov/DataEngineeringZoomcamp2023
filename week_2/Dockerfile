FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY homework_final.py /opt/prefect/flows/homework_final.py
COPY ./data /opt/prefect/data

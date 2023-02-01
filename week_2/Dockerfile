FROM prefecthq/prefect:s.7.7-puthon3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --reusted-host pypi.python.org --no-cache-dir

COPY 2_week /opt/prefect/flows
COPY data /opt/prefect/data


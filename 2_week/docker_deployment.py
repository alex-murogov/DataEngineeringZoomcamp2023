from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
# from 4_parameterized_flow.py import etl_gcs_to_bq  # TODO: change 4th file to have parent and child flows

etl_gcs_to_bq = __import__('4_parameterized_flow')


docker_block = DockerContainer.load("zoomcamp")
docker_dep = Deployment.build_from_flow(
    flow=etl_gcs_to_bq,
    name='docker-flow',
    infrastructure=docker_block
)


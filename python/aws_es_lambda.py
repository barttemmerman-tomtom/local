import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection

session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()

es_host = 'search-my-es-domain.eu-west-1.es.amazonaws.com'
awsauth = AWSRequestsAuth(
    aws_access_key=credentials.access_key,
    aws_secret_access_key=credentials.secret_key,
    aws_token=credentials.token,
    aws_host=es_host,
    aws_region=session.region_name,
    aws_service='es'
)

# use the requests connection_class and pass in our custom auth class
es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

print(es.info())
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth

# let's talk to our AWS Elasticsearch cluster
auth = AWSRequestsAuth(aws_access_key='ASIAQTDLK7BOIQY4YTO5',
                       aws_secret_access_key='TYhivlxabLKAC6LjeBcXjbR8tXvHvbkIUX7f2uuw',
                       aws_host='vpc-mdbf-es-t6jwuet4vy2gs5kibmgqdqp3li.eu-west-1.es.amazonaws.com',
                       aws_region='eu-west-1',
                       aws_service='es')
response = requests.get('http://vpc-mdbf-es-t6jwuet4vy2gs5kibmgqdqp3li.eu-west-1.es.amazonaws.com',
                        auth=auth)
print(response.content)

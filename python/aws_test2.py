import boto3
import botocore.credentials
from botocore.awsrequest import AWSRequest
from botocore.endpoint import URLLib3Session
from botocore.auth import SigV4Auth



params = '{"name": "hello"}'
headers = {
'Host': 'ram.ap-southeast-2.amazonaws.com',
}
request = AWSRequest(method="POST", url="https://ram.ap-southeast-2.amazonaws.com/createresourceshare", data=params, headers=headers)
SigV4Auth(boto3.Session().get_credentials(), "ram", "ap-southeast-2").add_auth(request)    


session = URLLib3Session()
r = session.send(request.prepare())

# auth = AWSRequestsAuth(aws_access_key='ASIAQTDLK7BOIQY4YTO5',
#                        aws_secret_access_key='TYhivlxabLKAC6LjeBcXjbR8tXvHvbkIUX7f2uuw',
#                        aws_host='vpc-mdbf-es-t6jwuet4vy2gs5kibmgqdqp3li.eu-west-1.es.amazonaws.com',
#                        aws_region='eu-west-1',
#                        aws_service='es')
# response = requests.get('http://vpc-mdbf-es-t6jwuet4vy2gs5kibmgqdqp3li.eu-west-1.es.amazonaws.com',
#                         auth=auth)

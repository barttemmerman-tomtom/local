import elasticsearch
import unicodedata
import csv
from requests_aws4auth import AWS4Auth
import sys
import os
import boto3
import time
import json
import argparse

HOST = 'vpc-mdbf-es-7vtlqskruaptymhymlwz7i3tcq.eu-west-1.es.amazonaws.com'
SEARCH_INDEX_PATTERN = "mdbf-prod-loggroup"
MAX_COUNT = 2.5 * 1000 * 1000

class ESClient:
    def __init__(self):
        self.es = None
        self.scroll_id = None

    def get(self):
        if not self.es:
            self.awsauth = self.aws_autorize()
            self.es = self.init_es(self.awsauth)

        return self.es

    def reset(self):
        self.es = None
        self.scroll_id = None

    def aws_autorize(self):
        print(f'authorizing aws session')
        session = boto3.Session()
        credentials = session.get_credentials()
        region = os.environ['AWS_DEFAULT_REGION']

        print(f'profile={session.profile_name} region={region}') 

        return AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
        
    def init_es(self, awsauth):
        print(f'initializing es client..')
        es = elasticsearch.Elasticsearch(
            hosts=[{'host': HOST, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=elasticsearch.RequestsHttpConnection
        )
        print(f'done - {es}')
        return es

    def search(self, body):
        print(f'searching (init)..')
        response = self.get().search(
            index = SEARCH_INDEX_PATTERN,
            scroll = '3m',
            body = body
        )
        #print(f'done search {response}')
        self.scroll_id = response['_scroll_id']
        return response

    def sleep_it_off(self, attempt):
        time.sleep( min(pow(5, attempt), 30))

    def next(self):

        if not self.scroll_id:
            raise RuntimeError("you can't next-search without a scroll_id")

        print(f'retreiving data..')

        attempt = 0
        while(True):

            attempt += 1
            try:
                return self.get().scroll(
                    scroll_id = self.scroll_id,
                    scroll = '5m'
                    )

            except elasticsearch.AuthorizationException as e:
                print(f'AuthorizationException exception during search-scroll (attempt:{attempt}): {e}, retrying..')
                
                # this doesn't count as an attempt
                attempt -= 1

                if e.status_code == 403:
                    self.reset()

                self.sleep_it_off(attempt)
                
            except Exception as e:

                # if attempt > 5:
                #     print(f'stopping retry after {attempt} attempts')
                #     raise e

                print(f'exception during search-scroll (attempt:{attempt}): {e} -- waiting')
                self.sleep_it_off(attempt)
                print(f'retrying..')

parser = argparse.ArgumentParser("argument parser")
parser.add_argument('--from-timestamp', help="start timestamp", type=str,)
parser.add_argument('--to-timestamp', help="end timestamp", type=str)
parser.add_argument('--outputfile', help="outputfile path", type=str)

args = parser.parse_args()
from_timestamp = args.from_timestamp
to_timestamp = args.to_timestamp
outputfile = args.outputfile

if not from_timestamp:
    print(f"missing 'from' timestamp")
    sys.exit(1)

if not to_timestamp:
    print(f"missing 'to' timestamp")
    sys.exit(1)

print(f'dumping documents from:{from_timestamp} to:{to_timestamp}')


body__ = {
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
            "range": {
                "@timestamp": {
                    "gte": from_timestamp,
                    "lt": to_timestamp
                }
            }
        }
      ]
    }
  },
  "sort": [
    {
        "@timestamp": {
        "order": "asc",
        "unmapped_type": "boolean"
        }
    }
  ]
}

es = ESClient()

with open(outputfile, 'w', encoding='utf-8') as f:

    record_counter = 0
    start_time = time.perf_counter()

    response = es.search(body__)

    while (True):
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        timed_out = response['timed_out']

        if len(hits) == 0:
            print(f'- no more results')
            break

        for hit in hits: 
            source = hit["_source"]
            print(json.dumps(source), file=f)

            record_counter += 1
            if record_counter % 10000 == 0:
                print(f'wrote {record_counter} records elapsed:{time.perf_counter() - start_time:0.4f}s')

        if record_counter % MAX_COUNT == 0:
            print(f'{MAX_COUNT} records processed, closing file..')
            break

        response = es.next()

    if record_counter % 10000 != 0:
        print(f'wrote {record_counter} records elapsed:{time.perf_counter() - start_time:0.4f}s')

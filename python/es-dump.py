import elasticsearch
import unicodedata
import csv
from requests_aws4auth import AWS4Auth
import sys
import os
import boto3
import time

HOST = 'vpc-mdbf-es-7vtlqskruaptymhymlwz7i3tcq.eu-west-1.es.amazonaws.com'
#SEARCH_INDEX_PATTERN = "mdbf-prod-loggroup-20*"
SEARCH_INDEX_PATTERN = "mdbf-prod-loggroup"

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

                time.sleep(5)
                
            except Exception as e:

                if attempt > 5:
                    print(f'stopping retry after {attempt} attempts')
                    raise e

                print(f'exception during search-scroll (attempt:{attempt}): {e}, retrying after 5s')
                time.sleep(5)

body__ = {
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
              "range": {
                "@timestamp": {
                    "gte": "2020-09-01T00:00:00.000Z",
                    "lt": "2020-10-01T00:00:00.000Z",
            }
          }
        },
        {
            "match_phrase": {
                "type": "CompetitorRouteLengthDifferenceEvent"
            }
        }
      ]
    }
  }
}

# # list all indices
# all_indices = es.indices.get_alias("*")
# print(f'dir = {dir(all_indices)}')
# [print(f'index: {index}') for index in all_indices]


# # scan search
# response = es.search(
#     index = "mdbf-prod-loggroup-202102",
#     scroll = '3m',
#     body = body__
#     )
# print(response)
# sys.exit(0)


es = ESClient()

with open('outputfile.tsv', 'w', newline='') as csvfile:   
    filewriter = csv.writer(
        csvfile,
        delimiter=';',
        quotechar='|', 
        quoting=csv.QUOTE_MINIMAL)

    header = ["projectName", "country", "region", "competitor", "sourceProvider", "stretchId","differenceInPercent","timestamp"]
    record_counter = 0
    # create column header row
    filewriter.writerow(header)
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
            filewriter.writerow(
                [
                    source["projectName"],
                    source["country"],
                    source["region"],
                    source["competitor"],
                    source["sourceProvider"],
                    source["stretchId"],
                    source["differenceInPercent"],
                    source["@timestamp"]
                    ]
                )
            record_counter += 1
            if record_counter % 10000 == 0:
                print(f'wrote {record_counter} records elapsed:{time.perf_counter() - start_time:0.4f}s')

        response = es.next()

    if record_counter % 10000 != 0:
        print(f'wrote {record_counter} records elapsed:{time.perf_counter() - start_time:0.4f}s')

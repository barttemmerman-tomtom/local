import boto3
import json
import psycopg2
import psycopg2.extras
import sys
import argparse
import os
import uuid

DB = {
    "prod": "prod-mdbf-db.c2mcxsumdn8z.eu-west-1.rds.amazonaws.com",
    "stage": "stage-mdbf-db.cribqwrzghuv.eu-west-1.rds.amazonaws.com"
}

TOPIC_ARN = {
    "prod" : "arn:aws:sns:eu-west-1:925373310607:mdbf-prod-stretch-created",
    "stage" : "arn:aws:sns:eu-west-1:041027303516:mdbf-prod-stretch-created"
}

environment = os.environ['ENVIRONMENT_NAME']
if not environment:
    print(f'aws environment not set, checking for ENVIRONMENT_NAME')
    sys.exit(1)

print(f'using environment={environment}')

# route-service: InspectionCompleteEvent
event = {
    "id": "{id}",
    "correlationId": "{correlationId}",
    "timestamp": "2021-04-26T00:00:00.000000Z",
    "stretchId": "{stretchId}",
    "geometry": "{geometry}",
    "stretchType": "SMALL",
    "country": "{country}",
    "region": "{region}",
    "projectName": "{projectName}",
    "routeProvider": "{routeProvider}",
    "type": "StretchCreated"
}


parser = argparse.ArgumentParser("argument parser")
parser.add_argument('--project-filter', help="project filter (str)", type=str)

args = parser.parse_args()
project_filter = args.project_filter

print(args)

sns_client = boto3.client('sns')


def get_correlation_id_for_stretch(stretch_id):
    return "{}{}".format(
        "00000000-0000-0000-0000-",
        stretch_id.split('-')[4])

def get_stretches(connection, projectFilter):
    try:

        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            count = 0
            cursor.execute(
                f"""
                select s.id, s.wkt, r.iso_country_code , r.region, p."name" project_name, r.provider 
                from routes.stretch s 
                join routes.route r on s.route_id = r.id and exists (select dp.state from routes.decision_point dp where dp.stretch_id = s.id)
                join routes.project p on p.id = r.project_id and p."name" ilike '{projectFilter}'
                where r.route_state = 'INIT'
                """
            )

            for record in cursor:
                yield (record['id'], 
                        record['wkt'], 
                        record['iso_country_code'],
                        record['region'],
                        record['project_name'],
                        record['provider'])

                if count % 100 == 0:
                    print(f"{count} stretches..")
                count+=1

            print(f"{count} total stretches")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
      
def compose_event(stretch_id, wkt, iso_country_code, region, project_name, provider):
    event["id"] = str(uuid.uuid4())
    event["correlationId"] = str(uuid.uuid4())
    event["stretchId"] = stretch_id
    event["geometry"] = wkt
    event["country"] = iso_country_code
    event["region"] = region
    event["projectName"] = project_name
    event["routeProvider"] = provider
    return event

def send_event(stretch_information):
    #print("sending event for stretchId={}".format(stretch_id))
    send_event = compose_event(*stretch_information)
    print(f"stretchId={str(stretch_information[0])}")

    response = sns_client.publish(
        TopicArn=TOPIC_ARN[environment],
        Message='{}'.format(json.dumps(send_event)),
    )
    print(f"response={response}")


with psycopg2.connect(user = "dba_admin",
                        password = "dba_admin",
                        host = DB[environment],
                        port = "5432",
                        database = "mdbf_db") as connection:

    print (connection.get_dsn_parameters(),"\n")

    print(f"project_filter={project_filter}")

    for stretch_information in get_stretches(connection, project_filter):
        # send_event = compose_event(stretch_id, run_id)
        print(f"sending StretchCreated stretch={stretch_information[0]} event={compose_event(*stretch_information)}")
        send_event(stretch_information)
        


import boto3
import json
import psycopg2
import psycopg2.extras
import sys
import argparse
import os


DB = {
    "prod": "prod-mdbf-db.c2mcxsumdn8z.eu-west-1.rds.amazonaws.com",
    "stage": "stage-mdbf-db.cribqwrzghuv.eu-west-1.rds.amazonaws.com"
}

TOPIC_ARN = {
    "prod" : "arn:aws:sns:eu-west-1:925373310607:mdbf-prod-evaluation-complete",
    "stage" : "arn:aws:sns:eu-west-1:041027303516:mdbf-stage-evaluation-complete"
}

environment = os.environ['ENVIRONMENT_NAME']
if not environment:
    print(f'aws environment not set, checking for ENVIRONMENT_NAME')
    sys.exit(1)

print(f'using environment={environment}')

# route-service: InspectionCompleteEvent
event = {
  "correlationId": "{correlationId}",
  "objectId": "{id}",
  "confidence": 1.0,
  "resultState": "VALID",
  "source": "MANUAL_FIX",
  "stretchType": "SMALL"
}

parser = argparse.ArgumentParser("argument parser")
parser.add_argument('--run-id', help="run UUID", type=str)
parser.add_argument('--all-stretches', action='store_true', help='send all stretches, if false it takes an arbitrary stretch and sendsd 1 event')

args = parser.parse_args()
run_id = args.run_id
send_all_stretches = args.all_stretches

print(args)

sns_client = boto3.client('sns')


def get_correlation_id_for_stretch(stretch_id):
    return "{}{}".format(
        "00000000-0000-0000-0000-",
        stretch_id.split('-')[4])

def route_id_generator(connection, run_id):
    try:

        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            count = 0
            cursor.execute(
                """
                select r.id
                from routes.route r 
                where run_id = '{run_id}'
                and r.route_state = 'INIT'
                """.format(run_id=run_id)
            )
            #cursor.itersize = 10
            for record in cursor:
                print(record['id'])
                yield record['id']

                if count % 100 == 0:
                    connection.commit()
                count+=1


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_arbitrary_stretch_id(connection, run_id):
    try:

        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            count = 0
            cursor.execute(
                """
                select s.id
                from routes.stretch s
                join routes.route r on r.id = s.route_id
                where r.run_id = '{rid}'
                limit 1
                """.format(rid=run_id)
            )

            for record in cursor:
                yield record['id']

                if count % 100 == 0:
                    print(record['id'])
                count+=1

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

        
def stretch_id_generator(connection, route_id):
    try:

        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

            count = 0
            cursor.execute(
                """
                select s.id
                from routes.stretch s 
                where s.route_id = '{rid}'
                and (exists (select 1 from routes.decision_point dp where dp.stretch_id = s.id and dp.route_id = s.route_id and dp.state = 'INIT')
                    or (select count(*) from routes.decision_point dp where dp.stretch_id = s.id and dp.route_id = s.route_id) = 0)
                """.format(rid=route_id)
            )
            #cursor.itersize = 10
            for record in cursor:
                print(record['id'])
                yield record['id']

                if count % 100 == 0:
                    connection.commit()
                count+=1

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def compose_event(stretch_id, correlation_id):
    event["objectId"] = stretch_id
    event["correlationId"] = correlation_id
    return event

def send_event(stretch_id, correlation_id):
    #print("sending event for stretchId={}".format(stretch_id))
    send_event = compose_event(stretch_id, correlation_id)
    print("event={}".format(str(send_event)))
    response = sns_client.publish(
        TopicArn=TOPIC_ARN[environment],
        Message='{}'.format(json.dumps(send_event)),
    )


with psycopg2.connect(user = "dba_admin",
                        password = "dba_admin",
                        host = DB[environment],
                        port = "5432",
                        database = "mdbf_db") as connection:

    print (connection.get_dsn_parameters(),"\n")

    if send_all_stretches:
        print("all INIT stretches")
        for route_id in route_id_generator(connection, run_id):
            for stretch_id in stretch_id_generator(connection, route_id):
                send_event(stretch_id, route_id)

    else:
        print("only 1 stretch dude.")
        for stretch_id in get_arbitrary_stretch_id(connection, run_id):
            # send_event = compose_event(stretch_id, run_id)
            print(f"sending InspectionCompleteEvent for stretch={stretch_id} event={compose_event(stretch_id, run_id)}")
            send_event(stretch_id, run_id)
        


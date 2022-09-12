import click
import requests
import boto3
# from requests_aws4auth import AWS4Auth


@click.group()
def main():
    pass


@main.command()
@click.option('--es-host', required=True, help='Specify the host of elasticsearch')
@click.option('--region', required=True, help='In which region es and snapshot bucket are located?')
def main(es_host, region):

    # credentials = boto3.Session().get_credentials()
    # awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)

    index = 'my-index-000001'
    print("Removing index {}".format(index))
    endpoint = 'https://{}/{}'.format(es_host, index)
    response = requests.delete(endpoint ) #, auth=awsauth)
    response.raise_for_status()


if __name__ == '__main__':
    main()
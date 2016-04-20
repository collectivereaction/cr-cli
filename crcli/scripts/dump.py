import click
import json
import boto3
import gzip
import xmltodict

client = boto3.client('dynamodb')

@click.command(short_help="Download a database into a gzipped file")
@click.argument('table', default='-', required=True)
@click.argument('filepath', default='dump.gz', required=False)
@click.option(
    '--csv/--no-csv',
    default=False,
    help="download as csv")
@click.pass_context
def dump(ctx, table, filepath, csv):
    """This command dumps the contents of a dynamodb table into a gzipped file.

    To get the dump of the game-test table in xml, do:

        $ cr dump 'game-test' ./path/to/dump.gz

    To get the dump of the pad-test table in csv, do:

        $ cr dump 'pad-test' ./path/to/dump.csv --csv
    """

    items = []

    response = client.scan(TableName=table)
    items += response.get('Items')

    while response.has_key('LastEvaluatedKey'):
        response = client.scan(TableName=table, ExclusiveStartKey=response.get('LastEvaluatedKey'))
        items += response.get('Items')

    if csv == True:
        itemstring = 'session,timestamp,P,A'
        for item in items:
            itemstring += item['session']['S'] + ',' + item['timestamp']['N'] + ',' + item['P']['S'] + ',' + item['A']['S'] + '\n'
    else:
        items = {'Items':{'item':items}}
        itemstring = xmltodict.unparse((items))
    
    with gzip.open(filepath, 'wb') as f_out:
        f_out.write(itemstring)
        f_out.close()

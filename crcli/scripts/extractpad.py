import click
import json
import boto3
import gzip
import xmltodict

client = boto3.client('dynamodb')

@click.command(short_help="Extract a csv of pad data and user reported events for use in H20")
@click.argument('filepath', default='dump.gz', required=False)
@click.pass_context
def extractpad(ctx, filepath):
    """This command extract a csv of pad data and user reported events for use in H20.

        $ cr extract-pad-pattern-data ./path/to/dump.gz
    """

    pad = []
    game = []

    response = client.query(TableName='game-test',QueryFilter={'event':{'AttributeValueList':[{'S':'badness'},{'S':'goodness'}],'ComparisonOperator':'EQ'}},ConditionalOperator='OR')
    items += response.get('Items')

    while response.has_key('LastEvaluatedKey'):
        response = client.query(TableName='game-test',QueryFilter={'event':{'AttributeValueList':[{'S':'badness'},{'S':'goodness'}],'ComparisonOperator':'EQ'}},ConditionalOperator='OR')
        items += response.get('Items')

    itemstring = 'session,timestamp,event'
    for item in items:
        itemstring += item['session']['S'] + ',' + item['timestamp']['N'] + ',' + item['event']['S'] + '\n'
    
    with gzip.open(filepath, 'wb') as f_out:
        f_out.write(itemstring)
        f_out.close()

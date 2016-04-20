import click
import json
import boto3
import gzip
import xmltodict

client = boto3.client('dynamodb')

def scanpad():
    items = dict()

    response = client.scan(TableName='pad-test')
    for item in response.get('Items'):
        if item['session']['S'] not in items:
            items[item['session']['S']] = []

        items[item['session']['S']].append(item)

    while response.has_key('LastEvaluatedKey'):
        response = client.scan(TableName='pad-test', ExclusiveStartKey=response.get('LastEvaluatedKey'))
        for item in response.get('Items'):
            if item['session']['S'] not in items:
                items[item['session']['S']] = []

            items[item['session']['S']].append(item)

    return items

def reactions_to_string(items, pad):
    reactionstring = ''
    for item in items:
        reaction = item['event']['S']
        session = item['id']['S']
        time = int(item['time']['N'])
        if reaction == 'badness' or reaction == 'goodness':
            reactionstring += session + ',' + reaction
            # Get all of the pad events associated with that reaction
            i = 0
            print session
            print pad.get(session)
            if session in pad:
                sessionpads = pad.get(session)
                print time
                while int(sessionpads[i]['timestamp']['N']) < time:
                    print int(sessionpads[i]['timestamp']['N'])
                    i += 1

                reactionpads = sessionpads[i-10:i]
                for pad in reactionpads:
                    reactionstring += ',' + pad['P']['S'] + ',' + pad['A']['S']
            
            reactionstring += '\n'
    
    return reactionstring


@click.command(short_help="Extract a csv of pad data and user reported events for use in H20")
@click.argument('filepath', default='dump.gz', required=False)
@click.pass_context
def extractpad(ctx, filepath):
    """This command extract a csv of pad data and user reported events for use in H20.

        $ cr extract-pad-pattern-data ./path/to/dump.gz
    """

    print 'Scanning pad table'
    pad = scanpad()

    print 'Scanning game table'
    response = client.scan(TableName='game-test')

    linestring = 'session,reaction,p1,a1,p2,a2,p3,a3,p4,a4,p5,a5,p6,a6,p7,a7,p8,a8,p9,a9,p10,a10\n'
    linestring = linestring + reactions_to_string(response.get('Items'), pad)

    while response.has_key('LastEvaluatedKey'):
        response = client.scan(TableName='game-test', ExclusiveStartKey=response.get('LastEvaluatedKey'))
        linestring = linestring + reactions_to_string(response.get('Items'), pad)

    with gzip.open(filepath, 'wb') as f_out:
        f_out.write(linestring)
        f_out.close()

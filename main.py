def hello(event, context):
    name = event['queryStringParameters']['name']
    message = 'Hello ' + name
    return {
        'statusCode': 200,
        'body': '{"message": "' + message + '"}'
    }

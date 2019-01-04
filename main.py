def list_datasets(event, context):
    return {
        'statusCode': 200,
        'body': '["1", "2", "3"]'
    }

def create_dataset(event, context):
    return {
        'statusCode': 201
    }

def get_dataset(event, context):
    dataset_id = event['pathParameters']['dataset-id']
    return {
        'statusCode': 200,
        'body': '{"id": "' + dataset_id + '"}'
    }

import boto
import boto.kinesis
import time

streamName = 'deployment'
kinesis = boto.kinesis.connect_to_region('us-west-2')

def list_streams(kinesis):
	streams = kinesis.list_streams()
	for stream in streams['StreamNames']:
		print(stream)

def push_message(kinesis):
	data = 'Some data ... ' + time.strftime("%c")
	record = {
            	'Data': data,
            	'PartitionKey': data,
        	}
	kinesis.put_record(streamName, data, '1')
	print('pushed a message to stream ', streamName)

def pull_messages(kinesis):
	response = kinesis.describe_stream(streamName)
	if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
		shard_id = response['StreamDescription']['Shards'][0]['ShardId']
	else:
		raise Error('Inactive stream')
	response = kinesis.get_shard_iterator(streamName, shard_id, 'TRIM_HORIZON')
	shard_iterator = response['ShardIterator']
	num_collected = 0
	tries = 0

	while tries < 25:
			tries += 1
			time.sleep(1)
			response = kinesis.get_records(shard_iterator)
			for record in response['Records']:
		                if 'Data' in record:
		                    print('', num_collected, record['Data'])
		                    num_collected += 1
			shard_iterator = response['NextShardIterator']
			#print(response)
	print('Messages retrieved: ', num_collected)

list_streams(kinesis)
push_message(kinesis)
pull_messages(kinesis)

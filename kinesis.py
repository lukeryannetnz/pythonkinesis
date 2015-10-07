import boto
import boto.kinesis
import time
import datetime
import uuid
import string

streamName = 'deployment'
kinesis = boto.kinesis.connect_to_region('us-west-2')

def list_streams(kinesis):
	streams = kinesis.list_streams()
	for stream in streams['StreamNames']:
		print(stream)

def create_new_release_message():
	with open ("./release-message.json", "r") as releaseTemplateFile:
		template = releaseTemplateFile.read().replace('\n', '')
	template = string.Template(template);
	message = template.substitute(id = uuid.uuid4(), title= 'a new release!', owner = 'luke', productId = uuid.uuid4(), statusId = uuid.uuid4(), typeId = uuid.uuid4(), jiraLink = 'http://jira.com/coolstuff', date = datetime.datetime.utcnow())
	return message

def push_messages(kinesis):
	data = create_new_release_message()
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
push_messages(kinesis)
pull_messages(kinesis)

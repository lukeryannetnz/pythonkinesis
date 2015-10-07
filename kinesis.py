import boto
import boto.kinesis
import time
import datetime
import uuid
import string

class messages:
	""" Contains logic to generate json messages for sending to kinesis """

	def create_new_release_message(self):
		with open ("./release-message.json", "r") as releaseTemplateFile:
			template = releaseTemplateFile.read().replace('\n', '')
		template = string.Template(template);
		message = template.substitute(id = uuid.uuid4(), title= 'a new release!', owner = 'luke', productId = uuid.uuid4(), statusId = uuid.uuid4(), typeId = uuid.uuid4(), jiraLink = 'http://jira.com/coolstuff', date = datetime.datetime.utcnow())
		return message

	def create_new_deployment_message(self):
		with open ("./deployment-message.json", "r") as deploymentTemplateFile:
			template = deploymentTemplateFile.read().replace('\n', '')
		template = string.Template(template);
		message = template.substitute(id = uuid.uuid4(), artifactName = 'MXWeb', version = '1.0.0.0', fullArtifact = 'XeroWeb-CI-v10.2.6.299.zip', userName = 'dev-luker', host = 'S1INTAPP02', date = datetime.datetime.utcnow())
		return message

class kinesisclient:
	""" A simple example of pushing to and pulling from a kinesis stream using the boto library """
	def __init__(self, stream):
		self.streamName = stream

	def connect(self, region):
		self.kinesis = boto.kinesis.connect_to_region(region)

	def print_streams(self):
		streams = self.kinesis.list_streams()
		for stream in streams['StreamNames']:
			print(stream)

	def push_messages(self):
		data = messages().create_new_release_message()
		self.kinesis.put_record(self.streamName, data, '1')

		data = messages().create_new_deployment_message()
		self.kinesis.put_record(self.streamName, data, '1')
		print('pushed 2 messages to stream ', self.streamName)

	def pull_messages(self):
		response = self.kinesis.describe_stream(self.streamName)
		if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
			shard_id = response['StreamDescription']['Shards'][0]['ShardId']
		else:
			raise Error('Inactive stream')
		response = self.kinesis.get_shard_iterator(self.streamName, shard_id, 'TRIM_HORIZON')
		shard_iterator = response['ShardIterator']
		num_collected = 0
		tries = 0

		while tries < 25:
				tries += 1
				time.sleep(1)
				response = self.kinesis.get_records(shard_iterator)
				for record in response['Records']:
							if 'Data' in record:
								print('', num_collected, record['Data'])
								num_collected += 1
				shard_iterator = response['NextShardIterator']

		print('Messages retrieved: ', num_collected)

### main ###
client = kinesisclient('deployment')
client.connect('us-west-2')
client.print_streams()
client.push_messages()
client.pull_messages()

#!/usr/local/bin/python3
from io import TextIOWrapper
from gzip import GzipFile
from csv import DictReader
import json
from datetime import datetime
import asyncio
import logging
import uuid
import traceback
import time

#from multiprocessing import Process, Pipe

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')

import boto3
import aioboto3

# Global vars
FIELDNAMES = (
    'logdate',  # this gets stripped and merged into a new timestamp field
    'logtime',  # this gets stripped and merged into a new timestamp field
    'edge-location',
    'src-bytes',
    'ip',
    'method',
    'host',
    'uri-stem',
    'status',
    'referer',
    'user-agent',
    'uri-query',
    'cookie',
    'edge-result-type',
    'edge-request-id',
    'host-header',
    'protocol',
    'resp-bytes',
    'time-taken',
    'forwarded-for',
    'ssl-protocol',
    'ssl-cipher',
    'edge-response-result-type',
    'protocol-version',
    'file-status',
    'encrypted-fields'
)

#sts client connection
sts_client = boto3.client('sts')
assumed_role_credentials = sts_client.assume_role(
    RoleArn="arn:aws:iam::123456789:role/role-for-kinesis",
    RoleSessionName="AssumeRoleSession1"
    )['Credentials']

async def consumer(n, q):
    # kinesis client connection

    # We wrap the entire code block here in a try to capture exceptions by
    # explicitly printing them
    # see https://medium.com/@yeraydiazdiaz/asyncio-coroutine-patterns-errors-and-cancellation-3bb422e961ff
    try:
        print('consumer {}: starting'.format(n))

        # create a async wrapped boto kinesis client
        async with aioboto3.client(
            'kinesis',
            aws_access_key_id=assumed_role_credentials['AccessKeyId'],
            aws_secret_access_key=assumed_role_credentials['SecretAccessKey'],
            aws_session_token=assumed_role_credentials['SessionToken'],
            # region_name='ap-southeast-2'
            ) as kinesis_client:
    
            print('consumer {}: got kinesis connection'.format(n))

            # do until we break
            while True:

                #print('consumer {}: waiting for item'.format(n))
                record_dict = await q.get()
                #print('consumer {}: has item'.format(n))

                # None is the signal to stop.
                if record_dict is None:
                    q.task_done()
                    break
                # Work normally
                else:
                    attempt = record_dict["attempt"]
                    records = record_dict["records"]
                    # If we already tried more times than we wanted stop attempting
                    if attempt >= 5:
                        print('{} records failed to retry'.format(len(records)))
                        q.task_done()
                        continue

                    # Sleep before retrying
                    if attempt:
                        await asyncio.sleep(2 ** attempt * .1)
                        # (0.1s, 0.2s, 0.4s, 0.8s, 1.60s, 3.20s, 6.40s, 12.80s, 25.60s, 51.20s, 102.40s...)

                    # Try to put the records
                    response = await kinesis_client.put_records(StreamName='prod-logs',
                                                                Records=records)
                    #print('consumer {}: put records'.format(n))

                    # Grab failed records
                    failed_record_count = response['FailedRecordCount']
                    if failed_record_count:
                        print('consumer {}: retrying failed record count {}'.format(n, failed_record_count))
                        failed_records = []
                        for i, record in enumerate(response['Records']):
                            if record.get('ErrorCode'):
                                failed_records.append(records[i])
                                await q.put({"records": failed_records, "attempt": attempt+1})

                    q.task_done()
        print('consumer {}: ending'.format(n))

    except BaseException as e:
        print('consumer {}: error: '.format(n))
        traceback.print_exc()


async def producer(q, num_workers, bucket, key):

    try:
        print('producer: starting')

        # s3 client connection
        s3_client = boto3.client('s3')
        print('producer: obtained s3 connection')
        print('producer: derived key {}'.format(key))

        # Grab the gziped logs and create a streaming object for them
        response = s3_client.get_object(Bucket=bucket, Key=key)
        gzipped = GzipFile(None, 'rb', fileobj=response['Body'])
        text = TextIOWrapper(gzipped)
        print('producer: created filestream')

        # Decode using CSV
        print('producer: decoding csv')
        csv_reader = DictReader(text, fieldnames=FIELDNAMES, dialect="excel-tab")
        next(csv_reader)
        next(csv_reader)

        record_chunk = []
        line_count = 0
        print('producer: adding records to queue')
        for line in csv_reader:
            line_count += 1
            # fix timestamp
            line['@timestamp'] = datetime.strptime(
                 line.pop('logdate') + " " + line.pop('logtime'), '%Y-%m-%d %H:%M:%S').astimezone().isoformat()
            #line['@timestamp'] = str(datetime.now().astimezone().isoformat())

            record = {
                'Data': json.dumps(line),
                'PartitionKey': uuid.uuid4().hex
            }

            record_chunk.append(record)

            #Maximum kinesis put records is 500
            if len(record_chunk) == 500:
                await q.put({"records": record_chunk, "attempt": 0})
                #print('producer: added task to the queue. queue depth: {}'.format(q.qsize()))
                record_chunk = []

        # If there are any leftover, add them to the queue
        if record_chunk:
            await q.put({"records": record_chunk, "attempt": 0})
            #print('producer: added task to the queue. queue depth: {}'.format(q.qsize()))

        print('producer: queued {} records'.format(line_count))

        # Add None entries in the queue
        # to signal the consumers to exit
        print('producer: adding stop signals to the queue')
        await q.join()
        await asyncio.sleep(10)
        for i in range(num_workers):
            await q.put(None)
        print('producer: waiting for queue to empty')
        await q.join()
        print('producer: ending')

    except BaseException as e:
        print('producer: error: {}'.format(e))
        traceback.print_exc()


async def main(loop, num_workers, bucket, key):
    # Create the queue with a fixed size so the producer
    # will block until the consumers pull some items out.
    q = asyncio.Queue(maxsize=num_workers)

    # Scheduled the consumer tasks.
    consumers = [
        loop.create_task(consumer(i, q))
        for i in range(num_workers)
    ]

    # Schedule the producer task.
    prod = loop.create_task(producer(q, num_workers, bucket, key))

    # Wait for all of the coroutines to finish.
    await asyncio.wait(consumers + [prod])


def lambda_handler(event, context):

    # Grab the bucket and the key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Set the number of async workers and create an async event loop
    num_workers = 50
    loop = asyncio.new_event_loop()
    aioboto3.setup_default_session(loop=loop)
    try:
        loop.run_until_complete(main(loop, num_workers, bucket, key))
    finally:
        loop.close()

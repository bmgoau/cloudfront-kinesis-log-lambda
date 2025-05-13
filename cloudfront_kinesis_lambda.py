#!/usr/local/bin/python3
import asyncio
import json
import traceback
import uuid
from csv import DictReader
from datetime import datetime
from gzip import GzipFile
from io import TextIOWrapper
from urllib.parse import unquote

import aioboto3
import boto3
from botocore.credentials import (
    DeferredRefreshableCredentials,
    create_assume_role_refresher,
)

# Global vars
FIELDNAMES = (
    "log_date",  # this gets stripped and merged into a new timestamp field
    "log_time",  # this gets stripped and merged into a new timestamp field
    "cf_pop",
    "bytes_sent",
    "src_ip",
    "http_method",
    "cf_distribution",
    "http_path",
    "http_status",
    "http_referer",  # this gets stripped
    "http_user_agent",
    "http_query",  # this gets stripped
    "http_cookie",  # this gets stripped
    "cf_result",
    "cf_request_id",
    "http_host",
    "http_protocol",
    "bytes_received",
    "duration_s",
    "xff",  # this gets stripped
    "tls_protocol",
    "tls_cipher",
    "cf_result_pre",
    "http_version",
    "fle-status",  # this gets stripped
    "fle-encrypted-fields",  # this gets stripped
    "src_port",
    "ttfb_s",
    "cf_result_detail",
    "http_content_type",
    "http_content_length",
    "http_range_start",  # this gets stripped
    "http_range_end",  # this gets stripped
)

# sts client connection used to create kinesis client
LOG_SESSION = boto3.Session()
STS_CLIENT = LOG_SESSION.client("sts")
CURRENT_ACCOUNT = STS_CLIENT.get_caller_identity().get("Account")
role_arn = "arn:aws:iam::123456789:role/role-for-kinesis"

LOG_SESSION._session._credentials = DeferredRefreshableCredentials(
    create_assume_role_refresher(
        STS_CLIENT,
        {
            "RoleArn": role_arn,
            "RoleSessionName": "CloudfrontLoggingFunction",
        },
    ),
    method="sts-assume-role",
)

MAX_RECORDS_PER_KINESIS_PUT = 500
NUM_WORKERS = 25


async def run_consumer(worker_number, queue, context):
    # kinesis client connection

    # We wrap the entire code block here in a try to capture exceptions by
    # explicitly printing them
    # see https://medium.com/@yeraydiazdiaz/asyncio-coroutine-patterns-errors-and-cancellation-3bb422e961ff
    try:
        # print(f"consumer {worker_number}: starting")

        session = aioboto3.Session(
            aws_access_key_id=LOG_SESSION.get_credentials().access_key,
            aws_secret_access_key=LOG_SESSION.get_credentials().secret_key,
            aws_session_token=LOG_SESSION.get_credentials().token,
        )

        # create a async wrapped boto kinesis client
        async with session.client("kinesis") as kinesis_client:
            # print(f"consumer {worker_number}: got kinesis connection")

            # do until we break
            while True:
                # print('consumer {}: waiting for item'.format(worker_number))
                # Consumer will block here until an item is available or
                # until queue.join() detects no more items in the queue
                # and  consumer.cancel() plus asyncio.gather cause CancelledError
                record_dict = await queue.get()
                # print('consumer {}: has item'.format(worker_number))

                # Process normally
                attempt = record_dict["attempt"]
                records = record_dict["records"]

                # Sleep before retrying
                if attempt:
                    wait_time = 2**attempt * 0.1

                    # don't wait if it's going to cause the function to time out
                    if context.get_remaining_time_in_millis() - wait_time * 1000 < 0:
                        queue.task_done()  # Avoid deadlock on queue.join()
                        continue

                    await asyncio.sleep(wait_time)
                    # (0.1s, 0.2s, 0.4s, 0.8s, 1.60s, 3.20s, 6.40s, 12.80s, 25.60s, 51.20s, 102.40s...)

                # Try to put the records
                response = await kinesis_client.put_records(StreamName="prod-logs", Records=records)
                # print('consumer {}: put records'.format(worker_number))

                # Grab failed records
                if failed_record_count := response.get("FailedRecordCount"):
                    # print(
                    #     f"consumer {worker_number}: retrying failed record count: {failed_record_count}"
                    # )

                    # The response Records will contain all successful and failed responses in the same order they were provided.
                    # Here we loop through the response and for the requests that failed we get those records from
                    # the original list and retry them.
                    failed_records = []
                    for i, result in enumerate(response["Records"]):
                        if error_code := result.get("ErrorCode"):
                            # print(
                            #     f"consumer {worker_number}: failed to PutRecord - {error_code} - {result.get('ErrorMessage')}"
                            # )
                            # Get the failed record from the attempted records
                            failed_record = records[i]
                            # Randomise the partition key again to avoid hitting the same partition
                            failed_record["PartitionKey"] = uuid.uuid4().hex
                            failed_records.append(failed_record)

                    await queue.put({"records": failed_records, "attempt": attempt + 1})

                queue.task_done()
    except asyncio.CancelledError:
        # print(f"consumer {worker_number}: exiting")
        pass
    except BaseException:
        print(f"consumer {worker_number}: error: ")
        traceback.print_exc()
        queue.task_done()  # Avoid deadlock on queue.join()


async def run_producer(queue, bucket, key):
    try:
        # print("producer: starting")

        # s3 client connection
        s3_client = boto3.client("s3")
        # print("producer: obtained s3 connection")

        # Key should look like
        # /stg/bf574f33-66e3-4936-a0b6-420325157173/EGI5P51QNDZ.2019-01-18-02.a3221b62.gz
        # Split it to derive some interesting information from it
        # /env/logging_id/file.gz
        print(f"producer: derived key {key}")
        split_key = key.split("/")
        log_environment = split_key[1]
        log_logging_id = split_key[2]

        # Grab the gziped logs and create a streaming object for them
        response = s3_client.get_object(Bucket=bucket, Key=key)
        gzipped = GzipFile(None, "rb", fileobj=response["Body"])
        text = TextIOWrapper(gzipped)
        # print("producer: created filestream")

        # Decode using CSV
        # print("producer: decoding tsv")
        csv_reader = DictReader(text, fieldnames=FIELDNAMES, dialect="excel-tab")
        # Skip the first 2 lines: version and field names
        next(csv_reader)
        next(csv_reader)

        record_chunk = []
        line_count = 0
        # print("producer: adding records to queue")
        for line in csv_reader:
            line_count += 1
            # strip some fields
            line.pop("http_referer", None)
            # fix timestamp
            line["@timestamp"] = (
                datetime.strptime(
                    line.pop("log_date") + " " + line.pop("log_time"),
                    "%Y-%m-%d %H:%M:%S",
                )
                .astimezone()
                .isoformat()
            )
            # line['@timestamp'] = str(datetime.now().astimezone().isoformat())
            # fix user_agent
            line["http_user_agent"] = unquote(line["http_user_agent"])
            # add service_id
            line["serviceId"] = log_logging_id
            line["type"] = "globaledge_cloudfront"
            line["env"] = log_environment

            record = {"Data": json.dumps(line), "PartitionKey": uuid.uuid4().hex}

            record_chunk.append(record)

            # Maximum kinesis put records is 500
            if len(record_chunk) == 500:
                # Pause if the queue depth is greater than NUM_WORKERS to limit memeory usage
                while queue.qsize() > 2 * NUM_WORKERS:
                    await asyncio.sleep(0.005)
                await queue.put({"records": record_chunk, "attempt": 0})
                # print('producer: added task to the queue. queue depth: {}'.format(q.qsize()))
                record_chunk = []

        # If there are any leftover, add them to the queue
        if record_chunk:
            await queue.put({"records": record_chunk, "attempt": 0})
            # print('producer: added task to the queue. queue depth: {}'.format(q.qsize()))

        print(f"producer: queued {line_count} records")

    except BaseException as e:
        print(f"producer: error: {e}")
        traceback.print_exc()


async def main(loop, bucket, key, context):
    # Warning: using a fixed size queue leads to deadlocks at peak hours, for reasons not fully understood
    queue = asyncio.Queue()
    # Schedule the consumer tasks.
    consumers = [
        loop.create_task(run_consumer(worker_number, queue, context))
        for worker_number in range(NUM_WORKERS)  # 25 consumer workers
    ]
    # Run the producer and wait for completion
    await run_producer(queue, bucket, key)
    # wait for consumers to finish processing all items
    await queue.join()
    # shut down the consumers
    for consumer in consumers:
        consumer.cancel()
    # Wait for all of the coroutines to finish.
    await asyncio.gather(*consumers, return_exceptions=True)


def lambda_handler(event, context):
    # Grab the bucket and the key from the event
    sns_event = json.loads(event["Records"][0]["Sns"]["Message"])
    bucket = sns_event["Records"][0]["s3"]["bucket"]["name"]
    key = sns_event["Records"][0]["s3"]["object"]["key"]

    # Create an async event loop if one doesn't exist.
    # Can't use asyncio.run() because sometimes the event loop is closed in Lambda.
    # See:
    # https://stackoverflow.com/questions/59293415/aws-lambda-with-python-asyncio-event-loop-closed-problem
    # https://stackoverflow.com/questions/60455830/can-you-have-an-async-handler-in-lambda-python-3-6#
    # https://stackoverflow.com/questions/43600676/what-is-the-correct-way-to-write-asyncio-code-for-use-with-aws-lambda
    # https://stackoverflow.com/questions/58193678/asyncio-is-not-working-within-my-python3-7-lambda
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(main(loop, bucket, key, context))

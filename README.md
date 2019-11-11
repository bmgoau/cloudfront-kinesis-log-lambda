# cloudfront-kinesis-log-lambda
Uses asynchronous boto3 (Lambdas do not support /dev/shm threading https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/) to retrieve Cloudfront tar gzipped logs in an S3 bucket and sends them to Kinesis. The lambda handler expects to receive a S3 object event as an invocation.

Requires: aioboto3, aiobotocore, aiohttp, async_generator, async_timeout, attr, chardet, idna, multidict, wrapt, yarl attr.py and dry_attr.py to run successfully in a Lambda 3.6

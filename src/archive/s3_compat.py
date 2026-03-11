import boto3


boto3.client("s3",endpoint_url="https://s3.amazonaws.com")

#write via put_object
s3 = boto3.client("s3",endpoint_url="https://s3.amazonaws.com")
s3.put_object(Bucket="my-bucket", Key="my-key", Body="Hello, S3!")

# unique sortable object keys with timestamp + UUID
import uuid
from datetime import datetime
timestamp = datetime.now().isoformat()
unique_key = f"{timestamp}_{uuid.uuid4()}"
s3.put_object(Bucket="my-bucket", Key=unique_key, Body="Hello, S3 with unique key!")


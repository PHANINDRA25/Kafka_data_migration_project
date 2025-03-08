from confluent_kafka import Consumer
import json
from s3fs import S3FileSystem
from datetime import datetime
import ast

conf = {'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'Y7YUKEAO3K6GAXII',
        'group.id': 'my_consumer_group',
        'sasl.password': 'd/k+JLejbE7KomhegdFmQBRcR+Bzafslr5ZA/YA1aPbdcCEiq4pz/IYCqgZhITKf'
        }

consumer=Consumer(conf)
s3=S3FileSystem()
topic = 'topic_22'  # replace with your topic name
consumer.subscribe([topic])
# count=0

import ast  # Import Abstract Syntax Tree module

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error:", msg.error())
        continue

    try:
        # Decode message
        value = msg.value().decode('utf-8')
        print("Received message:", value)

        # Convert string to proper JSON format using ast.literal_eval
        msg_value = ast.literal_eval(value)  # Converts it to a Python dictionary

        # Now, convert it to a JSON string
        msg_value = json.dumps(msg_value)  # Ensure proper JSON format

        # Generate unique filename
        filename = f's3://kafka-project-darshil34/stock_market_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json'

        print(f"Saving file to: {filename}")

        # Save to S3
        with s3.open(filename, 'w') as file:
            file.write(msg_value)  # Use `file.write()` instead of `json.dump()`

        print("File saved successfully!")

    except json.JSONDecodeError:
        print("Failed to decode JSON:", value)
    except Exception as e:
        print("Error writing to S3:", str(e))



# while True:
#     msg = consumer.poll(1.0)  # Poll for messages (1 sec timeout)

#     if msg is None:
#         continue  # No message received, continue looping
#     if msg.error():
#         print("Consumer error:", msg.error())
#         continue

#     try:
#         # Decode the message
#         value = msg.value().decode('utf-8')

#         # Replace single quotes with double quotes to make it valid JSON
#         value = value.replace("'", '"')

#         # Parse JSON
#         msg_value = json.loads(value)
        
#         # Save to S3
#         with s3.open(f's3://kafka-project-darshil34/stock_market_{count}.json', 'w') as file:
#             json.dump(msg_value, file)

#         print(f"Message {count} saved to S3")
#         count += 1

#     except json.JSONDecodeError:
#         print("Failed to decode JSON message:", value)

# # Close the consumer when done (use Ctrl+C to exit)
# consumer.close()
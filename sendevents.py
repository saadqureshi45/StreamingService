import boto3
import json
import uuid
import time
import datetime
import random

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis',
                              region_name='us-east-1',
                              aws_access_key_id='',
                              aws_secret_access_key=''
                              )
                              
                              
def generate_eventname():

    # Predefined lists of event types and subtypes
    event_types = ['account', 'lesson', 'payment', 'order', 'login']
    event_subtypes = ['created', 'started', 'completed', 'failed', 'logged_in', 'logged_out','order:completed']


    # Generate random event type and subtype
    event_type = random.choice(event_types)
    event_subtype = random.choice(event_subtypes)

    # Concatenate event type and subtype with ':'
    event_name = f"{event_type}:{event_subtype}"

    return event_name
     
def generate_events():
    # # Generate a unique event_uuid
    
        event_uuid = str(uuid.uuid4())
        created_at = int(time.time())
        event_name = [generate_eventname() for _ in range(100)]
        event_name= random.choice(event_name)
       
        
        json_data = {
            "event_uuid":event_uuid,
            "event_name":event_name,
            "created_at":created_at  
        
        }
          
        return json_data
        
def sendevents(events):
    # Put the record into the Kinesis stream
    response = kinesis_client.put_record(
        StreamName='event-data-stream',
        Data=json.dumps(events),
        PartitionKey='default'  # Use a static partition key for simplicity
    )
    return response
    


# Example usage:


for i in range(5):
    events=generate_events()
    print(events)
    
    response=sendevents(events)
    print("Event sent to Kinesis stream with sequence number:", response['SequenceNumber'])


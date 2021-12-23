import datetime
import logging
import os, json
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure.eventhub import EventHubProducerClient, EventData

class Constants:
    EVENTHUB_CONN_STR = os.environ['EventHubConfig']
    EVENTNAME_SOURCE = json.loads(os.environ['EventSource'])
    STORAGE_CONN_STR = os.environ['AzureWebJobsStorage']

def main(mytimer: func.TimerRequest) -> None:
    # Create an instance of blob storage
    container = ContainerClient.from_connection_string(conn_str=Constants.STORAGE_CONN_STR, container_name="stream-exception")
    # Get the list of items which is pending to push
    blob_list = container.list_blobs()
    # Iterate the items to send to eventhub
    for blob in blob_list:
         # Get particular failed item of blob client
         blob_client = container.get_blob_client(blob.name)
         # Download the blob in streams
         stream = blob_client.download_blob()
         # Read the data from streams
         data = stream.readall()
         # Load the data in json format
         req_body = json.loads(data)
         # Get the source system from payload json
         source_system = req_body.get('source').split('/')[0]
         logging.info(f'Source: {source_system}')
         logging.info(f'Event Source: {Constants.EVENTNAME_SOURCE}')
         # Extract the source system from config against source
         event_hub_name = Constants.EVENTNAME_SOURCE.get(source_system)
         try:
            # Create an instance of eventhub
            producer = EventHubProducerClient.from_connection_string(conn_str=Constants.EVENTHUB_CONN_STR, eventhub_name=event_hub_name)

            event_data_batch = producer.create_batch()
            # Add events to the batch.
            event_data_batch.add(EventData(data))

            # Send the batch of events to the event hub.
            producer.send_batch(event_data_batch)
                
            #msg.set(req_body)
            logging.info('Processed a request and sent data to event hub.')
            # Delete a blob after sending to eventhub
            blob_client.delete_blob()
         except Exception as ex:
            print(ex)
            logging.error('Exception:'+ str(ex))

    logging.info('Python timer trigger function ran at %s', str(datetime.datetime.utcnow()))
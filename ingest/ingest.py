#Posting events to EventHub
import logging
import json
import azure.functions as func
import OpenSSL.crypto
from OpenSSL.crypto import load_certificate, FILETYPE_ASN1
import base64
import os
import uuid
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Create eventhub client connections using event source
def get_eventhub_client_connections(eventhub_source, eventhub_conn_str):
        eventhub_clients ={}
        for eventhub in eventhub_source:
            eventhub_name = eventhub_source.get(eventhub) # Get eventhub name
            producer = EventHubProducerClient.from_connection_string(conn_str=eventhub_conn_str, eventhub_name=eventhub_name) # Create an instance
            print(f'Connection String: {eventhub_conn_str}/{producer.eventhub_name}')
            print(f'Auth URI:{producer._auth_uri}')
            eventhub_clients.update({eventhub:producer}) # Add each instance to dictionary
        return eventhub_clients

class Constants:
    REQUIRED_FIELDS = ['id','specversion','type','source','subject','time','data']
    THUMBPRINTS = os.environ['THUMBPRINTS'].lower().split(',')
    EVENTHUB_CONN_STR = os.environ['EventHubConfig']
    EVENT_SOURCE = json.loads(os.environ['EventSource'])
    STORAGE_CONN_STR = os.environ['AzureWebJobsStorage']
    FUNCTION_KEY = os.environ['APIKey']
    EVENTHUB_CONNECTIONS = get_eventhub_client_connections(EVENT_SOURCE, EVENTHUB_CONN_STR)
    
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function started processing a request.')
    
    # Check request has valid api key
    if(is_valid_apikey(req) == False):
        req_cert_str = req.headers.get("X-ARR-ClientCert") #read certificate from http req header
        if not req_cert_str:
            return func.HttpResponse('Certificate Not Found',status_code=400)
        if(is_valid_certificate(req_cert_str) == False):
            return func.HttpResponse('Unauthorized..',status_code=401)

    # Check http request body has data
    try:
        req_body = req.get_body().decode('utf-8')
    except Exception as ex:
        error_message = 'Request body empty'
        logging.error(error_message)
        return func.HttpResponse(error_message, status_code=400)
             
    # Validate that the request is json
    try:
        body = json.loads(req_body)
    except Exception as ex:
        error_message = f'Invalid json: {req_body}'
        logging.error(error_message)
        return func.HttpResponse(error_message, status_code=400)

    # Check for required fields
    missing_fields=[]
    for required_field in Constants.REQUIRED_FIELDS:
        if required_field not in body:
            missing_fields.append(required_field)

    if len(missing_fields) > 0:
        missing_fields = ", ".join(missing_fields)
        error_message = f'Missing the Following Required Fields: {missing_fields}'
        logging.error(error_message)
        return func.HttpResponse(error_message, status_code=400)
    
    # Post the requested data to eventhub
    try:
        source_system = body.get('source').split('/')[0]
        logging.info(f'Source System: {source_system}' )
        if send_message(req_body, source_system)  == False:
           return func.HttpResponse('Unable to push the message to storage', status_code=400) 
        return func.HttpResponse('Sent Successfully', status_code=200)
        
    except Exception as ex:
        print(ex)
        logging.error('Exception:'+ str(ex))
        return func.HttpResponse('Error while sending to event hub' + str(ex) ,status_code=400)

def send_message(req_body: str, source_system: str):

    # Read eventhub name from config based on source system
    try:
        # Create instance of eventhub client
        producer = Constants.EVENTHUB_CONNECTIONS.get(source_system)
        # Create event hub batch instance
        event_data_batch = producer.create_batch()
        # Add events to the batch.
        event_data_batch.add(EventData(req_body))
        # Send the batch of events to the event hub.
        producer.send_batch(event_data_batch)    
        #msg.set(req_body)
        logging.info('Processed a request and sent data to event hub.')
        return True
    except Exception as ex:
        print(ex)
        logging.error('Exception:'+ str(ex))
        try:
            # Create the BlobServiceClient object which will be used to create a container client
            blob_service_client = BlobServiceClient.from_connection_string(Constants.STORAGE_CONN_STR)
            # Get blob client
            container_client = blob_service_client.get_container_client('stream-exception')
            # Generate random GUID
            rand_guid = uuid.uuid4()
            file_name = f"{rand_guid}.json"
            # Upload a file to blob storage
            container_client.upload_blob(name=file_name, data=req_body)
            return True
        except Exception as ex:
            print(ex)
            logging.error('Exception in storage:'+ str(ex))
            return False 

def is_valid_certificate(req_cert_str: str):   
    try:
        # Read thumbprint from certificates              
        req_cert_bytes = base64.b64decode(req_cert_str) 
        cert = load_certificate(FILETYPE_ASN1, req_cert_bytes) 
        logging.info('Serial number: '+ str(cert.get_serial_number())) 

        sha1_fingerprint = cert.digest("sha1") 
        thumbprint = sha1_fingerprint.decode("utf-8").replace(':','').lower() 
        
        if not thumbprint: 
            logging.info('Thumbprint is null or empty.. ')
            return False

        # Validate thumbprint in the list
        if(thumbprint in Constants.THUMBPRINTS): 
            return True
        else:
            logging.info(f'Invalid thumbprint: {thumbprint}' )
            return False
    except Exception as ex: 
        print(ex)
        logging.error('Exception in is_valid_certificate:'+ str(ex))
        return False

def is_valid_apikey(req: func.HttpRequest):   
    try:
        # Read api key from headers              
        api_key = req.headers.get("api-key")

        # Check apikey is empty or not      
        if not api_key:
            logging.info('API key is null or empty.. ')
            return False
        else:
            logging.info('API Key : '+ api_key)

        # Validate api-key value with keyvault value
        if(api_key == Constants.FUNCTION_KEY):
            return True
        else:
            logging.info('Invalid API KEY...' + api_key)
            return False
    except Exception as ex:
        print(ex)
        logging.error('Exception in is_valid_apikey:'+ str(ex))
        return False
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
import json
from datetime import datetime
import os   
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "eventhubfervjestina"  #name of the event hub, connection string connects to eventspace where this event hub is
CONSUMER_GROUP = "$Default"  # Using  the default consumer group    
STORAGE_ACCOUNT_NAME = 'tpioudatalake'
STORAGE_ACCOUNT_KEY = 'qdKc5IaalOQYOkKx7IcYrsYhNa1FrefWeC9g+Zg3tivqjf1HD4iEpXMpoIsBX1gnIB3bSE1/qlqg+AStpKwdBg=='
FILE_SYSTEM_NAME = 'consumerdatalake'


service_client = DataLakeServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
                                       credential=STORAGE_ACCOUNT_KEY)

file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)

async def save_to_data_lake(event_data):
    # Extract creation time from the event data
    creation_time = datetime.fromtimestamp(event_data['created_utc'])
    directory_path = f"{creation_time:%Y/%m/%d/%H/%M}"
    file_name = f"{event_data['id']}.json"

    data_to_write = json.dumps(event_data)

    # Create directory hierarchy if it doesn't exist
    directory_client = file_system_client.get_directory_client(directory_path)
    directory_client.create_directory() 
    
    # Create a file and upload the content
    file_client = directory_client.get_file_client(file_name)
    file_client.create_file()

    file_size = file_client.get_file_properties().size
    
    logging.info("Appending data")
    file_client.append_data(data=data_to_write, offset=0, length=len(data_to_write)) 
    logging.info("Flushing data")
    file_client.flush_data(len(data_to_write))

async def on_event(partition_context, event):
    logger.info("Received event from partition {}".format(partition_context.partition_id))
    #Extract the event data
    event_data = event.body_as_json(encoding='UTF-8')

    try:
        if isinstance(event_data, dict):
            # Process a single post
            await save_to_data_lake(event_data)
        
        await partition_context.update_checkpoint(event)
        
    except json.JSONDecodeError as e:
        print(e)
        logging.error(f"Failed to decode event data: {e}")

async def main():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENT_HUB_NAME
    )

    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1",  # "-1" is from the beginning of the partition.
            )

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
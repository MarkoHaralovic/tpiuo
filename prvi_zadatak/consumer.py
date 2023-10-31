import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.filedatalake import DataLakeServiceClient
import json
from datetime import datetime
import os   
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "eventhubfervjestina"
CONSUMER_GROUP = "$Default"  # Using  the default consumer group    
DATA_LAKE_ACCOUNT_NAME = 'tpioudatalake'
DATA_LAKE_ACCOUNT_KEY = 'qdKc5IaalOQYOkKx7IcYrsYhNa1FrefWeC9g+Zg3tivqjf1HD4iEpXMpoIsBX1gnIB3bSE1/qlqg+AStpKwdBg=='
FILE_SYSTEM_NAME = 'consumerdatalake'

data_lake_service_client = DataLakeServiceClient(
    account_url=f"https://{DATA_LAKE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=DATA_LAKE_ACCOUNT_KEY
)

async def on_event(partition_context, event):
    try:
        data = json.loads(event.body_as_str(encoding="UTF-8"))
        created_utc = datetime.utcfromtimestamp(data['created_utc'])
        path = f"{created_utc.year}/{created_utc.month}/{created_utc.day}/{created_utc.hour}/{created_utc.minute}"
        
        file_system_client = data_lake_service_client.get_file_system_client(FILE_SYSTEM_NAME)
        directory_client = file_system_client.get_directory_client(path)
        await directory_client.create_directory()
        
        file_client = directory_client.get_file_client(f"{data['id']}.json")
        await file_client.create_file()
        await file_client.append_data(data=event.body_as_bytes(), offset=0, length=len(event.body_as_bytes()))
        await file_client.flush_data(len(event.body_as_bytes()))

        logger.info(f"Saved post {data['id']} to Data Lake at path: {path}")
        
    except Exception as e:
        logger.error(f"Failed to process event: {e}")

async def main():
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
    )

    async with consumer_client:
        await consumer_client.receive(on_event=on_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
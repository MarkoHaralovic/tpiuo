import asyncio
from azure.eventhub.aio import EventHubConsumerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "labos1_eventhub"
CONSUMER_GROUP = "$Default"  # Using  the default consumer group ??    


async def on_event(partition_context, event):
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )
 

consumer_client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    consumer_group=CONSUMER_GROUP,
    eventhub_name=EVENT_HUB_NAME,
)

try:
    asyncio.run(consumer_client.receive(on_event=on_event))
except KeyboardInterrupt:
    print("Receiving has stopped.")
finally:
    consumer_client.close()

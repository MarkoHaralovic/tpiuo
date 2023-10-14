import asyncio
import requests
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "labos1_eventhub"
REDDIT_URL = "https://www.reddit.com/r/dataengineering/top/.json?limit=10&t=all"
HEADERS = {
    "User-Agent": "Python/urllib"
}  # gotten from https://github.com/reddit-archive/reddit/wiki/API#rules


async def fetch_reddit_top_posts():
    response = requests.get(REDDIT_URL, headers=HEADERS)
    response.raise_for_status()
    return response.json().get("data", {}).get("children", [])


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    # code available at the link:https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=connection-string%2Croles-azure-portal
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        top_posts = await fetch_reddit_top_posts()
        event_data_batch = await producer.create_batch()
        # Add posts to the batch.
        for post in top_posts:
            print(post)
            post_data = post.get("data", {})
            event_data_batch.add(EventData(str(post_data)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)


asyncio.run(run())

while True:
    pass

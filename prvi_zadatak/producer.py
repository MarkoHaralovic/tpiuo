import asyncio
import requests
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "labos1_eventhub"
REDDIT_BASE_URL = "https://www.reddit.com/r/dataengineering/top/.json?t=all&limit=10"
HEADERS = {
    #limit could be added here, to reate a batch of 10 posts  
    "User-Agent": "Python/urllib"
}  
# gotten from https://github.com/reddit-archive/reddit/wiki/API#rules


async def fetch_reddit_top_posts(after=None):
    url = REDDIT_BASE_URL
    if after:
        url += f"&after={after}"
    
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json().get("data", {})
    return data.get("children", []), data.get("after")


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    # code available at the link:https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=connection-string%2Croles-azure-portal
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        after = None
        while True:
            top_posts, after = await fetch_reddit_top_posts(after)
            if not top_posts:
                break
            
            event_data_batch = await producer.create_batch()
            
            for post in top_posts:
                print(post)
                post_data = post.get("data", {})
                event_data_batch.add(EventData(str(post_data)))

            await producer.send_batch(event_data_batch)
            #creatin loop to send batch of 10 posts every 10 seconds
            await asyncio.sleep(10) 


asyncio.run(run())

while True:
    pass

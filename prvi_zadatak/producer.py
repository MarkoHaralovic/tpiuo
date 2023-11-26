import asyncio
import httpx
import requests
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import logging
import json

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=6QvWYyyNRIDg8FCqPOxoufaNl++n5EiGt+AEhLHqXuY=;EntityPath=labos1eventhub"
EVENT_HUB_NAME = "eventhubfervjestina"  #name of the event hub, connection string connects to eventspace where this event hub is
CONSUMER_GROUP = "$Default"  # Using  the default consumer group    
REDDIT_BASE_URL = "https://www.reddit.com/r/dataengineering/top/.json?t=all&limit=10"
HEADERS = {
    # limit could be added here, to reate a batch of 10 posts
    "User-Agent": "Python/urllib"
}
# gotten from https://github.com/reddit-archive/reddit/wiki/API#rules


async def fetch_reddit_top_posts(after=None):
    url = REDDIT_BASE_URL
    if after:
        url += f"&after={after}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            if data:
                # print("data")
                pass
            return data.get("data", {}).get("children", []), data.get("data", {}).get(
                "after"
            )
    except httpx.HTTPError as e:
        logger.error(f"Request failed: {e}")
        return [], None


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    # code available at the link:https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=connection-string%2Croles-azure-portal
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        after = None
        for _ in range(100):  # sending maximum of 1000 posts
            top_posts, _ = await fetch_reddit_top_posts(after)
            if not top_posts:
                break
            event_data_batch = await producer.create_batch()
            for post in top_posts:
                post_data = post.get("data", {})
                try:
                    # Serialize the post data and add it to the batch
                    event_data_batch.add(EventData(json.dumps(post_data)))
                except ValueError as e:
                    logger.info(f"Error adding post to batch: {e}")
                                
            await producer.send_batch(event_data_batch)
            logger.info(f"Sent batch of {len(top_posts)} posts to Event Hub.")
            await asyncio.sleep(
                10
            )  ##creating loop to send batch of 10 posts every 10 seconds


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("keyboardInterrupt")
        logger.info("Interrupted by user. Exiting...")
    except Exception as e:
        print(e)
        logger.error(f"An error occurred: {e}")
    while True:
        pass

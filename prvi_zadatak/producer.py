import asyncio
import aiohttp
import requests
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
EVENT_HUB_NAME = "eventhubfervjestina"
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
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=HEADERS) as response:
                response.raise_for_status()
                data = await response.json()
                if data:
                    print("data")
                return data.get("data", {}).get("children", []), data.get("data", {}).get("after")
        except aiohttp.ClientError as e:
            logger.error(f"Request failed: {e}")
            return [], None


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
            for _ in range(100):  #sending maximum of 1000 posts
                top_posts, after = await fetch_reddit_top_posts(after)
                print(f"After : {after}")
                if not top_posts:
                    break
                print("before batch")
                event_data_batch = await producer.create_batch()
                print("After batch")
                for post in top_posts:
                    print(f"Post : {post}")
                    post_data = post.get("data", {})
                    event_data_batch.add(EventData(str(post_data)))

                await producer.send_batch(event_data_batch)
                logger.info(f"Sent batch of {len(top_posts)} posts to Event Hub.")
                await asyncio.sleep(1)  ##creating loop to send batch of 10 posts every 10 seconds


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
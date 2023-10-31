from azure.eventhub import EventHubProducerClient, EventData

connection_str ="Endpoint=sb://labos1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fenUEfj8GMD1r5u9A36xbh0WqGOoKfHVh+AEhBeuyBM="
eventhub_name= "eventhubfervjestina"
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

try:
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData('Test message'))
    producer.send_batch(event_data_batch)
    print("Message sent successfully.")
except Exception as e:
    print(f"Error: {e}")

producer.close()

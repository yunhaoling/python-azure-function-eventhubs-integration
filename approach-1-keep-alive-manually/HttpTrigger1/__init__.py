import logging
import asyncio

import azure.functions as func
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

logging.basicConfig(level=logging.INFO)


class EventProducer:
    _MAX_NUM_RETRIES: int = 4
    _TIMEOUT_SEC: int = 5
    _KEEP_ALIVE_INTERVAL: int = 30

    def __init__(self, conn_str):
        self.client = EventHubProducerClient.from_connection_string(
            conn_str,
            retry_total=EventProducer._MAX_NUM_RETRIES,
            logging_enable=True,
            keep_alive_interval=EventProducer._KEEP_ALIVE_INTERVAL  # send heartbeat to the service to keep connection alive every 30s
        )
        self.lock = asyncio.Lock()

    async def produce(self, data):
        try:
            async with self.lock:
                logging.info(f'Start sending batch')
                await self.client.send_batch([EventData(data)], timeout=EventProducer._TIMEOUT_SEC)
                logging.info(f'Done sending batch')
            return
        except Exception as e:
            logging.error(f'Unexpected exception while sending data: {e}')
            logging.exception(e)

        logging.error('Unable to send the data: retries exhausted')

# producer singleton
conn_str = "<conn_str>"
producer = EventProducer(conn_str)


async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    await producer.produce('test')

    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )

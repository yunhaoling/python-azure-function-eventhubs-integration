import logging
import azure.functions as func


def main(req: func.HttpRequest) -> bytes:
    logging.info('Python HTTP trigger function processed a request.')
    return b'events'  # as defined in functions.json, the returned bytes would be wrapped into event data
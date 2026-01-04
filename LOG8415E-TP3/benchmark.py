import asyncio
import sys
import time
import aiohttp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NUMBER_OF_REQUESTS = 1000
APP_PORT=8000
ROUTES = ['directHit', 'randomHit', 'customHit']

async def read_task(gatekeeper_ip_address, route, session, request_num):
    headers = {'content-type': 'application/json'}
    try:
        read_query = "select * from actor where actor_id=1;"
        payload = {"auth_key":"tp3-log8415e", "query": f"{read_query}"}
        async with session.post(f"http://{gatekeeper_ip_address}:{APP_PORT}/{route}", json=payload, headers=headers) as response:
            status_code = response.status
            response_json = await response.json()
            response_data = response_json.get('data', {})
            logger.info(f" Request {request_num}: Status {status_code} for route {route} Response: {response_data}")
            return status_code, response_data
    except Exception as e:
        print(f" Request {request_num}: Failed - {str(e)}")
        return None, str(e)
    
async def write_task(gatekeeper_ip_address, route, session, request_num):
    headers = {'content-type': 'application/json'}
    write_query = f"update actor set first_name='Benchmark_route:{route}_request_num:{request_num}' where actor_id=1;"
    payload = {"auth_key":"tp3-log8415e", "query": f"{write_query}"}
    try:
        async with session.post(f"http://{gatekeeper_ip_address}:{APP_PORT}/{route}", json=payload, headers=headers) as response:
            status_code = response.status
            response_json = await response.json()
            response_data = response_json.get('data', {})
            logger.info(f" Request {request_num}: Status {status_code} for route {route} Response: {response_data}")
            return status_code, response_data
    except Exception as e:
        print(f" Request {request_num}: Failed - {str(e)}")
        return None, str(e)

async def benchmark(gatekeeper_ip_address):
    async with aiohttp.ClientSession() as session:
        for route in ROUTES:
            start_time = time.time()
            async with aiohttp.ClientSession() as session:
                task = [write_task(gatekeeper_ip_address, route, session, request_num) for request_num in range(NUMBER_OF_REQUESTS)]
                await asyncio.gather(*task)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Completed {NUMBER_OF_REQUESTS} write requests to route {route} in {duration:.2f} seconds.")
            logger.info(f"Throughput: {NUMBER_OF_REQUESTS / duration:.2f} requests/second")
            logger.info("--------------------------------------------------")
            
        for route in ROUTES:
            start_time = time.time()
            async with aiohttp.ClientSession() as session:
                task = [read_task(gatekeeper_ip_address, route, session, request_num) for request_num in range(NUMBER_OF_REQUESTS)]
                await asyncio.gather(*task)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Completed {NUMBER_OF_REQUESTS} read requests to route {route} in {duration:.2f} seconds.")
            logger.info(f"Throughput: {NUMBER_OF_REQUESTS / duration:.2f} requests/second")
            logger.info("--------------------------------------------------")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <gatekeeper_ip_address>")
    else:
        gatekeeper_ip_address = sys.argv[1]
        asyncio.run(benchmark(gatekeeper_ip_address))
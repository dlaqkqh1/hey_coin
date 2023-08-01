from pprint import pprint
import asyncio
import json
import aiohttp
import kafka_manage


TICKERS = ('KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-DOGE', 'KRW-ETC')
BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')


async def get_coin_price(ticker, session):
    async with session.get(f"https://api.upbit.com/v1/ticker?markets={ticker}") as response:
        response = await response.text()
        data = json.loads(response)[0]
        timestamp, trade_price = str(data['timestamp']), str(data['trade_price'])

        print(ticker, timestamp, trade_price)
        return ticker, timestamp, trade_price


async def send_coin_price_to_topic(ticker, key, value):
    producer = kafka_manage.get_producer(BROKERS)
    producer.send(
        topic=ticker,
        key=key.encode('utf-8'),
        value=value.encode('utf-8'))


async def main():
    async with aiohttp.ClientSession() as session:
        producer = kafka_manage.get_producer(BROKERS)

        while True:
            tasklist = [asyncio.ensure_future(get_coin_price(ticker, session)) for ticker in TICKERS]
            results = await asyncio.gather(*tasklist)
            tasklist = [asyncio.ensure_future(send_coin_price_to_topic(ticker, timestamp, trade_price)) for ticker, timestamp, trade_price in results]
            await asyncio.gather(*tasklist)
            producer.flush()
            await asyncio.sleep(0.5)


if __name__ == '__main__':
    for ticker in TICKERS:
        kafka_manage.create_topic(BROKERS, ticker)


asyncio.run(main())

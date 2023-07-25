from pprint import pprint
import asyncio
import json
import aiohttp


async def get_coin_price(session, ticker):
    async with session.get(f"https://api.upbit.com/v1/ticker?markets={ticker}") as response:
        response = await response.text()
        data = json.loads(response)[0]
        pprint(data)


async def main():
    ticker_list = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-DOGE', 'KRW-ETC']
    async with aiohttp.ClientSession() as session:
        for x in range(10):
            tasklist = [asyncio.ensure_future(get_coin_price(session, ticker)) for ticker in ticker_list]
            await asyncio.gather(*tasklist)
            await asyncio.sleep(0.5)

asyncio.run(main())


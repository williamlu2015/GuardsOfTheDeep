import asyncio

from src.data_extractor import DataExtractor


async def main():
    data_extractor = DataExtractor()
    await data_extractor.extract(2018, 8)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

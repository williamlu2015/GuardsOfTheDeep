import json
from calendar import monthrange

import aiohttp
import asyncio
import csv
import os


class DataExtractor:
    def __init__(self, api_key_num=0):
        self.api_key = None
        self.api_key_num = None
        self.session = None

        self._set_api_key(api_key_num)

    def _set_api_key(self, api_key_num):
        filename = f"../api/api_key_{api_key_num}.txt"
        if not os.path.isfile(filename):
            raise ValueError(f"API key #{api_key_num} does not exist")

        with open(filename, "r") as file:
            self.api_key = file.read()

        self.api_key_num = api_key_num
        print(f"API key set to #{api_key_num}")

    async def extract(self, to_year, to_month):
        async with aiohttp.ClientSession() as self.session:
            date_range = DateRange(to_year, to_month)
            while date_range.year >= 1999:
                await self._extract_date_range(date_range)
                date_range.decrement()

            await self._extract_date_range(OldDateRange())

    async def _extract_date_range(self, date_range):
        print(f"Started extracting {date_range.get_dirname()}")

        os.makedirs(f"../data/{date_range.get_dirname()}", exist_ok=True)

        num_pages = await self._get_num_pages(date_range)
        futures = [
            self._extract_page(date_range, index)
            for index in range(1, num_pages + 1)
        ]
        await asyncio.gather(*futures)

        print(
            f"Finished extracting {date_range.get_dirname()} "
            + f"({num_pages} pages)"
        )

    async def _get_num_pages(self, date_range):
        response = await self._request_page(date_range, 1)
        return response["response"]["pages"]

    async def _extract_page(self, date_range, index):
        filename = f"../data/{date_range.get_dirname()}/page_{index}.csv"
        with open(filename, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=",")
            self._write_header(csv_writer)

            response = await self._request_page(date_range, index)

            for article in response["response"]["results"]:
                self._write_article(article, csv_writer)

    @staticmethod
    def _write_header(csv_writer):
        header = [
            "apiUrl", "bodyText", "byline", "charCount", "firstPublicationDate",
            "headline", "lastModified", "productionOffice", "sectionId",
            "starRating", "trailText", "webUrl", "wordCount"
        ]
        csv_writer.writerow(header)

    async def _request_page(self, date_range, index):
        url = "https://content.guardianapis.com/search"
        fields = [
            "bodyText", "byline", "charCount", "firstPublicationDate",
            "headline", "lastModified", "productionOffice", "starRating",
            "trailText", "wordcount"
        ]
        params = {
            "api-key": self.api_key,
            "page": index,
            "page-size": 200,
            "order-by": "oldest",
            "show-fields": ",".join(fields),
            "to-date": date_range.get_last_day()
        }
        if not isinstance(date_range, OldDateRange):
            params["from-date"] = date_range.get_first_day()

        curr_api_key = params["api-key"]
        async with self.session.get(url, params=params, ssl=False) as response:
            if response.status == 429:
                return (
                    await self._rerequest_page(date_range, index, curr_api_key)
                )

            assert response.status == 200, response

            text = await response.text()
            result = json.loads(text)
            return result

    async def _rerequest_page(self, date_range, index, curr_api_key):
        print(f"API key {curr_api_key}: rate limit exceeded")
        if self.api_key == curr_api_key:
            self._set_api_key(self.api_key_num + 1)

        return await self._request_page(date_range, index)

    @staticmethod
    def _write_article(article, csv_writer):
        row = [
            article["apiUrl"],
            article["fields"].get("bodyText"),
            article["fields"].get("byline"),
            article["fields"].get("charCount"),
            article["fields"].get("firstPublicationDate"),
            article["fields"].get("headline"),
            article["fields"].get("lastModified"),
            article["fields"].get("productionOffice"),
            article["sectionId"],
            article["fields"].get("starRating"),   # defaults to None
            article["fields"].get("trailText"),
            article["webUrl"],
            article["fields"].get("wordcount")
        ]
        csv_writer.writerow(row)


class DateRange:
    def __init__(self, year, month):
        self.year = year
        self.month = month

        self.last_day = None
        self._update_last_day()

    def get_first_day(self):
        return f"{self.year}-{self.month}-1"

    def get_last_day(self):
        return f"{self.year}-{self.month}-{self.last_day}"

    def get_dirname(self):
        return f"{self.year}-{self.month}"

    def decrement(self):
        if self.month == 1:
            self.year -= 1
            self.month = 12
        else:
            self.month -= 1

        self._update_last_day()

    def _update_last_day(self):
        self.last_day = monthrange(self.year, self.month)[1]


class OldDateRange:
    @staticmethod
    def get_last_day():
        return "1998-12-31"

    @staticmethod
    def get_dirname():
        return "old"

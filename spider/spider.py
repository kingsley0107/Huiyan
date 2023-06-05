# -*- coding: utf-8 -*-
"""
Created on 04 Jun 4:28 PM

@Author: kingsley leung
@Email: kingsleyl0107@gmail.com

_description_: async crawl huiyan poi data
"""

import asyncio
import json
import re
import time

import aiohttp
import geopandas as gpd
import pandas as pd
import transbigdata as tbd
from configs.static_vars import (
    POI_SAVE_PATH,
    YEARS,
    CITIES,
    POI_TYPE_ONE,
    POI_TYPE_TWO,
    HUIYAN_COOKIE,
)


class Huiyan_Crawler:
    def __init__(self) -> None:
        self.base_url = "https://huiyan.baidu.com/"
        self.poi_url = "https://huiyan.baidu.com/block/poihot.jsonp"
        self.session = aiohttp.ClientSession()
        self._user_configuration()

    def _user_configuration(self):
        self.cookie = HUIYAN_COOKIE
        self.User_Agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        self.host = "huiyan.baidu.com"
        self.headers = {
            "User-Agent": self.User_Agent,
            "Cookie": self.cookie,
            "Host": self.host,
        }

    async def _close_session(self):
        await self.session.close()

    async def request_url(self, url, params=None):
        """通用request请求器

        Args:
            url (_type_): _待爬url_
            params (_type_, optional): _params_. Defaults to None.

        Returns:
            _type_: _description_
        """
        async with self.session.get(
            url, headers=self.headers, params=params
        ) as async_response:
            return await async_response.read()

    @staticmethod
    def process_poi_response(poi_response):
        poi_content_raw = poi_response.decode()
        useful_poi_content = json.loads(re.sub(r"\w+\((.*)\)$", r"\1", poi_content_raw))
        poi_data = useful_poi_content["data"]["gridhots"]
        poi_data_df = pd.DataFrame(poi_data)
        poi_data_df["x"], poi_data_df["y"] = poi_data_df["x"].astype(
            "int32"
        ), poi_data_df["y"].astype("int32")

        poi_data_df["x"], poi_data_df["y"] = tbd.bd09towgs84(
            *tbd.bd09mctobd09(poi_data_df["x"], poi_data_df["y"])
        )
        poi_data_df["geometry"] = gpd.points_from_xy(poi_data_df["x"], poi_data_df["y"])
        poi_gdf = gpd.GeoDataFrame(poi_data_df, geometry="geometry", crs="EPSG:4326")
        return poi_gdf

    @staticmethod
    def save_gdf(gdf, save_path):
        gdf.to_file(save_path)

    async def crawl_poi2file(self, cityId, year, poiOne, poiTwo, save=True):
        file_name = poiTwo if poiTwo else poiOne if poiOne else "all_poi"
        print(f"start crawling {cityId}_{file_name}_{year}!")
        poi_params = {
            "cityId": cityId,
            "year": year,
            "districtCode": "",
            "isCenter": 0,
            "poiOne": poiOne,
            "poiTwo": poiTwo,
        }

        poi_response = await self.request_url(self.poi_url, params=poi_params)
        poi_gdf = self.process_poi_response(poi_response)
        save_path = f"{POI_SAVE_PATH}/{cityId}_{file_name}_{year}.geojson"
        if save:
            self.save_gdf(poi_gdf, save_path)
        number_of_poi = self.cal_poi_num(poi_gdf)
        print(
            f"\nCITY:{cityId}\nYEAR:{year}\n{poiOne}_{poiTwo}_NUMBER:{number_of_poi}\n"
        )
        return poi_gdf

    @staticmethod
    def cal_poi_num(poi_gdf):
        return poi_gdf["num"].sum()


async def schedule():
    crawler = Huiyan_Crawler()
    tasks = []
    for city in CITIES:
        for year in YEARS:
            task = asyncio.ensure_future(
                crawler.crawl_poi2file(
                    city, year, POI_TYPE_ONE, POI_TYPE_TWO, save=True
                )
            )
            tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)
    await crawler._close_session()


def main():
    start_time = time.time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(schedule())
    print("--- %s seconds ---" % (time.time() - start_time))

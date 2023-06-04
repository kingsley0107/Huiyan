import requests
import aiohttp
import asyncio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import json
import pandas as pd
import transbigdata as tbd
import geopandas as gpd
from static_vars import POI_SAVE_PATH, YEARS, CITIES


class Huiyan_Crawler:
    def __init__(self) -> None:
        self.base_url = "https://huiyan.baidu.com/"
        self.poi_url = "https://huiyan.baidu.com/block/poihot.jsonp"
        self.session = requests.Session()
        self._user_configuration()
        self._session_configuration()

    def _user_configuration(self):
        self.cookie = """BIDUPSID=8605C2866E10907D4FD38CEE1D9D8EF1; PSTM=1659877476; H_WISE_SIDS_BFESS=110085_131861_203518_204918_209307_211986_213041_213357_214805_215730_216850_219559_219623_219943_219946_222216_222624_223063_223323_226006_226628_227870_227932_228650_229154_229968_230930_231979_232357_234044_234050_234208_234295_234308_234315_234426_234725_234802_234927_235092_235174_235200_235443_235473_235512_235714_235832_235979_236237_236242_236307_236497_236515_236537_236611_236810_237093_237240_237833_237835_238265_238410_238429_238507_238514_238630_238755_238949_238981_239007_239101_239150_239281_239396_239500_239549_239569_239605_239636_239705_239857_239900_239947_240014_240049_240306_240340_240398_240405_240408_240447_240465_240597_240650_240743_240783_240790_241178_241208_241248_241297_241432; BAIDUID=3F7F3F921A24F3CD4CD24EDC63F8705E:FG=1; BDHYUSS=iHxInMy5cENiIbGG6VeKpMcCeiRiBGZP; greenHand=true; BAIDUID_BFESS=3F7F3F921A24F3CD4CD24EDC63F8705E:FG=1; ZFY=4ns3lj7gU7TWTF:Bb:Bh2rcWNPofuE1pIDBEEn8nXRLkg:C; newlogin=1; Hm_lvt_d3737ab3e5e90097fc9ff85a463fa01d=1684151665,1685346955; H_WISE_SIDS=219946_216838_213345_214801_219943_213034_230173_204913_110085_236312_243881_244726_240590_245412_253427_253213_255982_107311_252560_256083_255660_254831_256739_251973_256620_257078_257289_254317_257744_257941_257586_258244_257996_255231_258374_258369_258724_258881_258938_257302_258985_258958_230288_259049_257015_259191_259193_259277_256225_259290_259218_259288_256999_259558_259652_251786_259705_258772_234295_234208_259910_259643_260034_254300_259241_256229_260357_259721_260364_260228_259186_253022_255212_260158_258081_260331_260697_260584_260834_259312_259422_259585_260717_261043_261028_257822_259758_261122_261375_253900_261562_260466_261661_261705_261718_260651_261628_261824_261863_261894_261864_261999_262062_255910_262065_259033_262172_262184_260441_262026_261800; __bid_n=183cb1b1cff89781cb4207; SECKEY_ABVK=SgTvvBX5eV77Sc9CTl1Il8ydpg9woMXkAth4IDdDU2g%3D; BMAP_SECKEY=o78VBo4NRozDH59L0MbQRHAlM0C0e_egbhbLPbeVe38Yr8oKX7AlHwg3eDbZC1AWRTj4K4wA4hVdx8hkzIYcnHNZhPEx7kvgokt_PHrndAi5Kox3mFW5XjA00l0-UQ-aX7XU0B8v1jUde2r7hOyiTSvq8GJQO2RuinCZCXzF9lmU5bMoZVNiUTMfrbTpHama; Hm_lvt_11bf4c06cfe39caa5e953c383e912d19=1685346746,1685684048; FPTOKEN=yDeytwPQWajknto1Aq6WT5545nUuMlDIJcRPCnudOVBwIvIMq+81biMhKGh7+dgHxWKAqg7tmUQu+Pp2XFpEOX4GO9BOCpRougiXNIRuWEnaY6qoe/ys+s3KbZ4lmCGrUSRByjbNAQgEXDKxfZWdVqpz/dVEW922ykDvXAVJ3poga+Z8kAZfZvGEaPEHu3w72Dxi+9Rct76dSNLi6REh6IYIE5LSLcpXbNA0QR6ILzGb9tEFBlpLgoX5PNZ81G4zHBuNf2/GpewbPJrwVoG6Pprbzi5lw26Gw8TaJFxz+GpT4V9cQs8FurWB8Wi4iiI1qkUpENKEJb00Xb5CR9pDq5m3Y88lYYHDZCTtU6AHnXMWjFLheY1wDQAxJlNGJK8YdhxxYQOiruxXWIJQKRWopA==|VbjH5LvVmo46r1/1T29t++yoTcRL+B8yu5bR/TfwPPs=|10|af28410037dc71d81a5f67b202ef391e; RT="z=1&dm=baidu.com&si=bc528b8a-8c73-4e5f-a514-f3145bdae8fd&ss=liepspli&sl=1&tt=59i&bcn=https%3A%2F%2Ffclog.baidu.com%2Flog%2Fweirwood%3Ftype%3Dperf&ld=62a&ul=7ux&hd=7wc"; PHPSESSID=tek5dkiectc8uodkpd29v0lvu6; Hm_lvt_e8002ef3d9e0d8274b5b74cc4a027d08=1685676017,1685684048,1685842576,1685843825; cityCode=340; cityName=%u6DF1%u5733; Hm_lpvt_e8002ef3d9e0d8274b5b74cc4a027d08=1685843866; ab_sr=1.0.1_MjNkYjIxMGM0YjBmNWRjOWU5MzAwYmYzN2UwZDZlM2Q5MDA0OGFmYzliMjM2MzM2NGI5NWZlMDUxYmYxZjRhMzcxNDIyY2QxZDM3NDA1NzQyYzJiMjM1M2UwYWQ3NTUxNDEwMTc1YzAzMDJjMDZlMmQzYmQxODA5Y2M5MzA0MGIwM2ExOWU5MzMzODQ3NmQxM2RhNTc1YTBiMmY0ZmE1Mg=="""
        self.User_Agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        self.host = "huiyan.baidu.com"
        self.headers = {
            "User-Agent": self.User_Agent,
            "Cookie": self.cookie,
            "Host": self.host,
        }

    def _session_configuration(self):
        # 保证每个实例维持完整会话
        retry_times = 5
        retry_delay = 1
        retry = Retry(total=retry_times, backoff_factor=retry_delay)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def request_url(self, url, params=None):
        """通用request请求器

        Args:
            url (_type_): _待爬url_
            params (_type_, optional): _params_. Defaults to None.

        Returns:
            _type_: _description_
        """
        request_response = self.session.get(url, headers=self.headers, params=params)
        return request_response

    @staticmethod
    def process_poi_response(poi_response):
        poi_content_raw = poi_response.text
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

    def crawl_poi2file(self, cityId, year, poiOne, poiTwo, save=True):
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
        poi_response = self.request_url(self.poi_url, params=poi_params)
        poi_gdf = self.process_poi_response(poi_response)
        save_path = f"{POI_SAVE_PATH}/{cityId}_{file_name}_{year}.geojson"
        if save:
            self.save_gdf(poi_gdf, save_path)

        return poi_gdf

    @staticmethod
    def cal_poi_num(poi_gdf):
        return poi_gdf["num"].sum()


def main():
    test = Huiyan_Crawler()
    POI_TYPE_ONE = "公共设施"
    POI_TYPE_TWO = "医疗设施"
    for city in CITIES:
        for year in YEARS:
            poi_gdf = test.crawl_poi2file(
                city, year, POI_TYPE_ONE, POI_TYPE_TWO, save=False
            )
            number_of_poi = test.cal_poi_num(poi_gdf)
            print(
                f"\nCITY:{city}\nYEAR:{year}\n{POI_TYPE_ONE}_{POI_TYPE_TWO}_NUMBER:{number_of_poi}\n"
            )


if __name__ == "__main__":
    import cProfile
    import pstats

    with cProfile.Profile() as pr:
        main()
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats(filename="needs_profiling.prof")

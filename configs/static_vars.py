import os

CITY_MAP = {"深圳": 340, "佛山": 138, "东莞": 119, "惠州": 301}
# POI保存路径
POI_SAVE_PATH = r"./poi"
os.makedirs(POI_SAVE_PATH, exist_ok=True)

# 待爬年份及城市编号
# 年份最早到2015
YEARS = [2018, 2019, 2020, 2021, 2022]
CITIES = [340, 138, 119, 301]

# from poi_type_manual 前两列
# 若需要所有种类，输入 "" 即可
POI_TYPE_ONE = "公共设施"
POI_TYPE_TWO = "医疗设施"

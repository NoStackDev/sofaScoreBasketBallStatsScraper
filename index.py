import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from pprint import pprint
from datetime import date, timedelta
import time
import pickle

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "api.sofascore.com",
    "Origin": "https://www.sofascore.com",
    "Pragma": "no-cache",
    "Referer": "https://www.sofascore.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Sec-GPC": "1",
    "TE": "trailers",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0",
}


class Scraper:
    def __init__(self, url: str, headers, use_proxy_rotation: bool = False):
        self.url = url
        self.headers = headers
        self.use_proxy_rotation = use_proxy_rotation
        self.proxy_list_url = "https://free-proxy-list.net/"
        self.proxies = []
        self.proxy_index = 0

        if use_proxy_rotation:
            self.get_proxy_list()

    def fetch(self, url: str) -> requests.models.Response:
        if len(self.proxies) > 0:
            for proxy in self.proxies:
                print("trying: ", proxy, end="...")
                try:
                    response = requests.get(
                        url,
                        headers=self.headers,
                        proxies={"https": proxy},
                        timeout=5,
                    )
                    print(" working proxy")
                    if self.proxy_index >= len(self.proxies) - 1:
                        self.proxy_index = 0
                        self.get_proxy_list()
                        print("aaaaaaaaaaaaaa", end="    ")
                    else:
                        self.proxy_index += 1
                        print("bbbbbbbbbbbb", end="    ")
                    return response
                except:
                    print(" bad proxy")
                    if self.proxy_index >= len(self.proxies) - 1:
                        self.proxy_index = 0
                        self.get_proxy_list()
                        self.fetch(url)
                        print("ccccccccccccccccccc", end="    ")
                    else:
                        self.proxy_index += 1
                        print("dddddddddddddddd", end="    ")

        response = requests.get(url, headers=self.headers)
        return response

    def parse(self, response: requests.models.Response) -> BeautifulSoup:
        return BeautifulSoup(response.text, "html.parser")

    def get_proxy_list(self) -> list:
        print("fetching fresh proxy list...", end=" ")
        self.proxies = []
        response = self.fetch(self.proxy_list_url)
        soup = self.parse(response)
        tbody = soup.find("tbody")
        trs = tbody.find_all("tr")
        for tr in trs:
            try:
                tds = tr.find_all("td")
                if (
                    tds[4].string.lower().strip() == "elite proxy"
                    and tds[6].string.lower().strip() == "yes"
                ):
                    self.proxies.append(
                        "https://" + tds[0].string.strip() + ":" + tds[1].string.strip()
                    )
            except:
                pass
        print("done")
        print(self.proxies)

    def flatten_dict_gen(self, obj: dict, prev_key: str = ""):
        for k, v in obj.items():
            new_key = k if prev_key == "" else prev_key + "_" + k
            if isinstance(v, dict):
                yield from self.flatten_dict(v, new_key).items()
            else:
                yield new_key, v

    def flatten_dict(self, obj: dict, prev_key="") -> dict:
        return dict(self.flatten_dict_gen(obj, prev_key))

    def get_saved_state(self) -> dict:
        try:
            with open("current.json", "r") as f:
                # Read the file contents and parse the JSON data
                data = json.load(f)
                return data
        except FileNotFoundError:
            with open("current.json", "w") as f:
                f.write(
                    json.dumps(
                        {
                            "scraped_dates": [],
                        }
                    )
                )
            return self.get_saved_state()

    def current_state_to_json(self, current_state: dict) -> dict:
        with open("current.json", "w") as f:
            f.write(json.dumps(current_state))
        return current_state

    def get_pickled_df(self) -> pd.DataFrame:
        try:
            df = pd.read_pickle("my_data.pkl")
            return df
        except FileNotFoundError:
            df = pd.DataFrame({})
            return df

    def pickle_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.to_pickle("my_data.pkl")
        return df

    def create_new_df_column(self, df: pd.DataFrame, keys) -> pd.DataFrame:
        columns = df.columns
        for key in keys:
            if key not in columns:
                df[key] = None

        return df

    def date_to_scrap(self, scraped_dates: list, time_obj: date = date.today()) -> str:
        delta = 1
        time_str = (time_obj - timedelta(days=delta)).strftime("%Y-%m-%d")

        while time_str in scraped_dates:
            delta += 1
            time_str = (time_obj - timedelta(days=delta)).strftime("%Y-%m-%d")

        return time_str

    def run(self):
        df = self.get_pickled_df()
        scraped_dates = self.get_saved_state()["scraped_dates"]
        time_str = self.date_to_scrap(scraped_dates=scraped_dates)

        while time_str != "2020-01-01":
            time.sleep(5)
            response = self.fetch(self.url + time_str)
            response_json: dict = json.loads(response.text)
            games: dict = response_json["events"]

            for game in games:
                if (
                    game["status"]["code"] == 100
                    or game["status"]["code"] == 110
                    and not df["id"].eq(game["id"]).any()
                ):
                    df = self.create_new_df_column(df, self.flatten_dict(game).keys())
                    df.loc[len(df)] = {**self.flatten_dict(game)}

            df = self.pickle_df(df)
            scraped_dates.append(time_str)
            current_state = self.current_state_to_json({"scraped_dates": scraped_dates})
            time_str = self.date_to_scrap(scraped_dates=current_state["scraped_dates"])
            print(df)



if __name__ == "__main__":
    process = Scraper(
        url="https://api.sofascore.com/api/v1/sport/basketball/scheduled-events/",
        headers=HEADERS,
    )
    process.run()

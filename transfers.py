from collections import defaultdict
from datetime import datetime
from itertools import batched
from main import BASE_URI, fetch_html_content
from zoneinfo import ZoneInfo

import aiofiles
import aiohttp
import asyncio
import json

from bs4 import BeautifulSoup
from tqdm import tqdm


TRANSFERS_URI = BASE_URI + "/transfers/"


def extract_transfer_data(row):
    row_info = row.find_all("td")

    (transfer_date, player, _, position, team, _) = row_info

    transfer_month, transfer_year = transfer_date.text.split("/")

    player_id = player.find("a", href=lambda href: href and "player_summary" in href)["href"].split("/")[2]

    team_id = team.find("a", href=lambda href: href and "teams" in href)["href"].split("/")[2]

    return transfer_month, transfer_year, player_id, position.text, team_id


async def fetch_transfers_data(session, competition):
    transfers_content = await fetch_html_content(session, TRANSFERS_URI + f"{competition}/")

    transfers_soup = BeautifulSoup(transfers_content, "html.parser")

    transfers = defaultdict(list)

    for div in transfers_soup.find_all("div", class_="data"):
        table = div.find("table", class_="standard_tabelle")

        if table:
            in_td = table.find("td", string="In")
            in_tr = None

            if in_td:
                in_tr = in_td.find_parent()

            out_td = table.find("td", string="Out")
            out_tr = None

            if out_td:
                out_tr = out_td.find_parent()

            if out_tr:
                for tr in out_tr.find_next_siblings():
                    (transfer_month, transfer_year, player, position, team) = extract_transfer_data(tr)

                    transfers[team].append({
                        "transfer_type": "departure",
                        "transfer_month": transfer_month,
                        "transfer_year": transfer_year,
                        "player": player,
                        "position": position,
                    })

            if in_tr:
                for tr in in_tr.find_next_siblings():
                    if tr == out_tr:
                        break

                    (transfer_month, transfer_year, player, position, team) = extract_transfer_data(tr)

                    transfers[team].append({
                        "transfer_type": "arrival",
                        "transfer_month": transfer_month,
                        "transfer_year": transfer_year,
                        "player": player,
                        "position": position,
                    })

    return transfers


async def main():
    result = {}

    async with aiofiles.open("competitions.txt", "r") as f:
        competitions = [line.strip() for line in await f.readlines()]

        with tqdm(total=None, desc="obtendo dados de transferências ", unit=" time") as pbar:
            async with aiohttp.ClientSession() as session:
                for chunk in batched(competitions, 15):
                    for transfers in await asyncio.gather(*[fetch_transfers_data(session, competition) for competition in chunk]):
                        result.update(transfers)
                        pbar.update(1)

    tz = ZoneInfo("America/Sao_Paulo")

    async with aiofiles.open(f"transfers-{datetime.now(tz)}.json", "w") as f:
        await f.write(json.dumps(result))

if __name__ == "__main__":
    asyncio.run(main())

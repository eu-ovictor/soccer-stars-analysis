import aiofiles
import aiohttp
import asyncio
import json
import os
import re

from bs4 import BeautifulSoup
from collections import defaultdict
from main import fetch_html_content
from tqdm import tqdm

BASE_URI = "https://www.transfermarkt.com.br"


def worth_in_euros(worth):
    worth_in_euros = 0.0

    match worth:
        case _ if "mi." in worth:
            worth_in_euros = float(worth.replace("mi. €", "").replace(",", ".").strip()) * 1_000_000
        case _ if "mil" in worth:
            worth_in_euros = float(worth.replace("mil €", "").replace(",", ".").strip()) * 1_000
        case _ if "bilhôes" in worth:
            worth_in_euros = float(worth.replace("bilhões €", "").replace(",", ".").strip()) * 1_000_000_000

    return worth_in_euros


async def fetch_leagues(session, countries):
    for country in countries:
        content = await fetch_html_content(session, BASE_URI + country["link"])

        soup = BeautifulSoup(content, "html.parser")

        h2 = soup.find("h2", string=lambda string: string and string.strip() == "Ligas & Copas nacionais")

        if h2:
            div = h2.find_parent("div", class_="box")

            a = div.find("a", href=lambda href: href and "startseite" in href)

            if a:
                yield {
                    "league_name": a["href"].split("/")[1],
                    "league_link": a["href"],
                    "league_country": country["name"],
                }


async def fetch_clubs_season_market_value(session, link, season):
    club_season_content = await fetch_html_content(session, BASE_URI + link)

    club_season_soup = BeautifulSoup(club_season_content, "html.parser")

    table = club_season_soup.find("table", class_="items")

    clubs_season_market_value = []

    if table:
        tbody = table.find("tbody")

        if tbody:
            for tr in tbody.find_all("tr"):
                try:
                    (team, _, _, _, _, _, market_value) = tr.find_all("td")
                except ValueError:
                    try:
                        team = tr.find("td")

                        team_icon = team.find("img")

                        team_name = team.find("a")

                        clubs_season_market_value.append({
                            "icon": team_icon["src"],
                            "team": team_name["title"].lower().replace(" ", "-"),
                            "season": season,
                            "market_value_in_euros": 0.0,
                        })

                        continue
                    except Exception:
                        continue

                team_icon = team.find("img")

                team_name = team.find("a")

                market_value_in_euros = worth_in_euros(market_value.text)

                clubs_season_market_value.append({
                    "icon": team_icon["src"] if team_icon else "",
                    "team": team_name["title"].lower().replace(" ", "-") if team_name else "",
                    "season": season,
                    "market_value_in_euros": market_value_in_euros,
                })

    return clubs_season_market_value


async def fetch_clubs_market_value(session, league):
    if league:
        clubs_content = await fetch_html_content(session, BASE_URI + league["league_link"])

        clubs_soup = BeautifulSoup(clubs_content, "html.parser")

        a = clubs_soup.find("a", href=lambda href: href and "marktwert_gesamt_anzeige" in href)

        if a:
            clubs_detail_content = await fetch_html_content(session, BASE_URI + a["href"])

            clubs_detail_soup = BeautifulSoup(clubs_detail_content, "html.parser")

            seasons = clubs_detail_soup.find("select", {"name": "saison_id"})

            season_link_r = re.compile(r"/saison_id/\d+/plus/")

            base_season_link = clubs_detail_soup.find("a", href=season_link_r)

            seasons_links = []

            if base_season_link:
                for option in seasons.find_all("option"):
                    season = option["value"]

                    link_args = base_season_link["href"].split("/")
                    link_args[6] = season

                    link = "/".join(link_args)

                    seasons_links.append((season, link))

            for clubs_season_market_value in await asyncio.gather(*[fetch_clubs_season_market_value(session, link, season) for (season, link) in seasons_links]):
                yield clubs_season_market_value
    else:
        yield []


async def main():
    with tqdm(
        total=None, desc="obtendo dados de valor de mercado dos clubes", unit=" informação de clubes por temporada"
    ) as pbar:
        async with aiofiles.open("countries.json", "r") as inf:
            async with aiohttp.ClientSession() as session:
                countries = json.loads(await inf.read())

                async for league in fetch_leagues(session, countries):
                    file_name = league["league_name"]

                    result = defaultdict(lambda: defaultdict(list))

                    async for clubs_market_value in fetch_clubs_market_value(session, league):
                        for club_market_value in clubs_market_value:
                            result[league["league_name"]][club_market_value["team"]].append(
                                {
                                    "icon": club_market_value["icon"],
                                    "season": club_market_value["season"],
                                    "market_value_in_euros": club_market_value["market_value_in_euros"],
                                    "country": league["league_country"]
                                }
                            )

                        pbar.update(len(clubs_market_value))

                    async with aiofiles.open(f"marktdata/{file_name}.json", "w") as of:
                        await of.write(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())

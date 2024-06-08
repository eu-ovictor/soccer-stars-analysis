import aiofiles
import aiohttp
import asyncio
import json

from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm
from zoneinfo import ZoneInfo

BASE_URI = "https://www.worldfootball.net/"

BASE_DIR = "./national_competitions"

CONFEDERATIONS = ["caf", "conmebol", "concacaf", "uefa", "afc", "ofc"]


async def fetch_html_content(session, url):
    async with session.get(url) as response:
        return await response.text()


async def fetch_competition(session, link):
    return await fetch_html_content(session, BASE_URI + link), link.split("/")[2]


async def fetch_season_overall_attendance(session, link, season):
    return await fetch_html_content(session, BASE_URI + link), season


async def fetch_overall_attendances(session, competition_content):
    competition_soup = BeautifulSoup(competition_content, "html.parser")

    main_attendance_link = competition_soup.find("a", href=lambda href: href and "attendance" in href)

    if not main_attendance_link:
        return {}

    main_attendance_content = await fetch_html_content(session, BASE_URI + main_attendance_link["href"])

    main_attendance_soup = BeautifulSoup(main_attendance_content, "html.parser")

    seasons_overall_attendances_links = [
        (option["value"], option.text)
        for option in main_attendance_soup.find_all("option", value=lambda value: value and "attendance" in value)
    ]

    team_stats = defaultdict(list)

    for season_overall_attendances_content, season in await asyncio.gather(
        *[
            fetch_season_overall_attendance(session, link, season)
            for (link, season) in seasons_overall_attendances_links
        ]
    ):
        season_overall_attendances_soup = BeautifulSoup(season_overall_attendances_content, "html.parser")

        table = season_overall_attendances_soup.find("table", class_="standard_tabelle")

        for a in table.find_all("a", href=lambda href: href and "teams" in href):
            if not a.find("img"):
                team = a.text

                stats = a.find_parent().find_next_siblings("td", limit=3)

                (sum, matches, average) = stats

                team_stats[team].append(
                    {
                        season: {
                            "sum": sum.text,
                            "matches": matches.text,
                            "average": average.text,
                        }
                    }
                )

    return team_stats


async def fetch_competition_stats(session, confederation, pbar):
    stats = {}

    async with aiofiles.open(f"{BASE_DIR}/{confederation}.html", mode="r") as f:
        content = await f.read()

        soup = BeautifulSoup(content, "html.parser")

        competition_links = [
            link["href"] for link in soup.find_all("a", href=lambda href: href and "competition" in href)
        ]

        for competition_content, competition_name in await asyncio.gather(
            *[fetch_competition(session, link) for link in competition_links]
        ):
            overall_attendances = await fetch_overall_attendances(session, competition_content)

            stats[competition_name] = overall_attendances

            pbar.update(1)

    return stats, confederation


async def main():
    result = {}

    with tqdm(total=None, desc="obtendo dados de público em competições", unit=" competição") as pbar:
        async with aiohttp.ClientSession() as session:
            for stats, confederation in await asyncio.gather(
                *[fetch_competition_stats(session, confederation, pbar) for confederation in CONFEDERATIONS]
            ):
                result[confederation] = stats

    tz = ZoneInfo("America/Sao_Paulo")

    async with aiofiles.open(f"attendances-{datetime.now(tz)}.json", "w") as f:
        await f.write(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())

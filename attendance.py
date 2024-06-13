import aiofiles
import aiohttp
import asyncio
import json

from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

BASE_URI = "https://www.worldfootball.net/"

CONFEDERATIONS = ["caf", "conmebol", "concacaf", "uefa", "afc", "ofc"]


async def fetch_html_content(session, url):
    async with session.get(url) as response:
        return await response.text()


async def fetch_national_leagues(session, confederation):
    national_leagues_content = await fetch_html_content(session, BASE_URI + confederation)

    national_leagues_soup = BeautifulSoup(national_leagues_content, "html.parser")

    tables = national_leagues_soup.find_all("table", class_="standard_tabelle")

    if tables:
        table = tables[-1]

        for national_league in table.find_all("a", href=lambda href: href and "competition" in href):
            yield national_league["href"]


async def fetch_seasons(session, national_league):
    national_league_content = await fetch_html_content(session, BASE_URI + national_league)

    national_league_soup = BeautifulSoup(national_league_content, "html.parser")

    a = national_league_soup.find("a", href=lambda href: href and "attendance" in href)

    if a:
        overall_attendance_link_parts = a["href"].split("/")

        overall_attendance_link_parts[3] = "3"

        overall_attendance_link = "/".join(overall_attendance_link_parts)

        overall_attendance_content = await fetch_html_content(session, BASE_URI + overall_attendance_link)

        overall_attendance_soup = BeautifulSoup(overall_attendance_content, "html.parser")

        return [
            (option["value"], option.text)
            for option in overall_attendance_soup.find_all(
                "option", value=lambda value: value and "attendance" in value
            )
        ]

    return []


async def fetch_attendance(session, link, season):
    attendance_content = await fetch_html_content(session, BASE_URI + link)

    attendance_soup = BeautifulSoup(attendance_content, "html.parser")

    table = attendance_soup.find("table", class_="standard_tabelle")

    attendance = []

    if table:
        for a in table.find_all("a", href=lambda href: href and "teams" in href):
            if not a.find("img"):
                team = a["href"].split("/")[2]

                stats = a.find_parent().find_next_siblings("td", limit=3)

                (sum, matches, average) = stats

                attendance.append(
                    (
                        team,
                        {
                            season: {
                                "sum": sum.text,
                                "matches": matches.text,
                                "average": average.text,
                            }
                        },
                    )
                )

        return attendance

    return []


async def main():
    with tqdm(total=None, desc="obtendo dados de público em competições", unit=" competição") as pbar:
        async with aiohttp.ClientSession() as session:
            home_content = await fetch_html_content(session, BASE_URI)

            home_soup = BeautifulSoup(home_content, "html.parser")

            confederations_links = [
                a["href"]
                for a in home_soup.find_all(
                    "a",
                    href=lambda href: href
                    and "continents" in href
                    and any(confederation in href for confederation in CONFEDERATIONS),
                )
            ]

            result = defaultdict(list)

            for confederation_link in confederations_links:
                async for national_league in fetch_national_leagues(session, confederation_link):
                    result = defaultdict(list)

                    seasons = await fetch_seasons(session, national_league)

                    for teams_attendance in await asyncio.gather(
                        *[fetch_attendance(session, link, season) for link, season in seasons]
                    ):
                        for team, attendance in teams_attendance:
                            result[team].append(attendance)

                    file_name = national_league.split("/")[2]

                    async with aiofiles.open(f"attendance/{file_name}.json", "w") as of:
                        await of.write(json.dumps(result))

                    pbar.update(1)


if __name__ == "__main__":
    asyncio.run(main())

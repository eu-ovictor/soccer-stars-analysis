import json
import requests

from bs4 import BeautifulSoup

BASE_URI = "https://www.worldfootball.net/"

RESULT = {}

with open("conmebol.html") as f:
    soup = BeautifulSoup(f, "html.parser")

    competition_links = [link["href"] for link in soup.find_all('a', href=lambda href: href and 'competition' in href)]

    for competition_link in competition_links:
        competition = competition_link.split('/')[2]

        response = requests.get(BASE_URI + competition_link) 

        attendance_soup = BeautifulSoup(response.content, "html.parser")

        attendance_link = attendance_soup.find('a', href=lambda href: href and 'attendance' in href)

        if not attendance_link:
            RESULT[competition] = {}

            continue

        response = requests.get(BASE_URI + attendance_link["href"])

        attendances_soup = BeautifulSoup(response.content, "html.parser")

        overall_attendances_link = attendances_soup.find('a', href=lambda href: href and 'attendance' in href, string="overall")

        if not attendance_link:
            RESULT[competition] = {}

            continue

        response = requests.get(BASE_URI + overall_attendances_link["href"])

        overrall_attendances_soup = BeautifulSoup(response.content, "html.parser")

        seasons_overall_attendances_links = [(option["value"], option.text) for option in overrall_attendances_soup.find_all('option', value=lambda value: value and 'attendance' in value)]

        for seasons_overall_attendances_link, season in seasons_overall_attendances_links:
            response = requests.get(BASE_URI + seasons_overall_attendances_link)

            season_overall_attendances_soup = BeautifulSoup(response.content, "html.parser")

            table = season_overall_attendances_soup.find('table', class_='standard_tabelle')

            team_stats = {}

            for a in table.find_all('a', href=lambda href: href and 'teams' in href):
                if not a.find("img"):
                    team = a.text

                    stats = a.find_parent().find_next_siblings('td', limit=3)

                    (sum, matches, average) = stats

                    team_stats[team] = {
                        "sum": sum.text,
                        "matches": matches.text,
                        "average": average.text
                    }

            RESULT[competition] = {
                season: team_stats
            }

    print(RESULT)

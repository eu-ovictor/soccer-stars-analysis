async def fetch_html_content(session, url):
    async with session.get(url) as response:
        return await response.text()

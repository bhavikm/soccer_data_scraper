import csv
import pickle
from datetime import datetime

import requests
import tqdm as tqdm
from bs4 import BeautifulSoup
import re
from pprint import pprint


# Set the header so that they think the request is coming from a website
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 503, 504),
    session=None,
):
    """Better way to do a HTTP request with retries"""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_page_soup(page_link):
    """For a page link, get the HTTP response and return BeautifulSoup tree"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
    tree = requests_retry_session().get(page_link, headers=headers)
    soup = BeautifulSoup(tree.content, 'html.parser')
    return soup


def extract_team_links(year):
    """Extract team links for a year"""
    page = 'https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=' + year
    soup = get_page_soup(page)

    # Create an empty list to assign the team links to
    teamLinks = []

    # Extract all links with the correct CSS selector
    links = soup.select("a.vereinprofil_tooltip")

    # We need the location that the link is pointing to, so for each link, take the link location.
    for i in range(len(links)):
        teamLinks.append(links[i].get("href"))
        
    # Remove repeats and un-needed links
    teamLinks = list(set(teamLinks))
    pattern = re.compile('startseite')
    teamLinks = [s for s in teamLinks if pattern.search(s) ]

    # For each location that we have taken, add the website before it - this allows us to call it later
    for i in range(len(teamLinks)):
        teamLinks[i] = "https://www.transfermarkt.co.uk" + teamLinks[i]

    return teamLinks


def extract_team_player_links(year_team_links):
    """Extract player links for a year and team"""
    team_player_links = {}  # team -> list of player links

    # Run the scraper through each of our 20 team links
    for team_link in year_team_links:
        # club from link
        team = team_link.split('/')[3]

        # Download and process the team page
        soup = get_page_soup(team_link)

        # Extract all links
        links = soup.select("a.spielprofil_tooltip")

        # Create an empty list for our player links to go into
        playerLinks = []

        # For each link, extract the location that it is pointing to
        for j in range(len(links)):
            playerLinks.append(links[j].get("href"))

        # The page list the players more than once - let's use list(set(XXX)) to remove the duplicates
        playerLinks = list(set(playerLinks))

        team_player_links[team] = playerLinks

    return team_player_links


def clean_extract_player_data(player_link, player_data):
    """From the extracted player data, return dict of cleaned data"""
    # player name from link
    player_name = " ".join(player_link.split('/')[3].split('-'))

    if 'Date of birth' in player_data:
        try:
            dob = datetime.strptime(player_data['Date of birth'], '%b %d, %Y')
        except ValueError:
            try:
                dob = datetime.strptime(player_data['Date of birth'], '%b %d, %Y Happy Birthday')
            except ValueError:
                dob = None
    else:
        dob = None

    if dob is not None:
        dob = dob.strftime('%d/%m/%Y')

    if 'Height' in player_data:
        height = player_data['Height'].encode('ascii', 'ignore')
        height = height[:-1].decode("utf-8").replace(',', '')
    else:
        height = None

    if 'Foot' in player_data:
        foot = player_data['Foot']
    else:
        foot = None
    if 'Citizenship' in player_data:
        citizenship = player_data['Citizenship']
    else:
        citizenship = None
    if 'Position' in player_data:
        position = player_data['Position']
    else:
        position = None

    return {
        'player_name': player_name,
        'dob': dob,
        'height': height,
        'foot': foot,
        'citizenship': citizenship,
        'position': position
    }


def scrape_player_data(team_year_player_link):
    """Scrape the player data, clean it and return cleaned data"""
    # Grab the page
    player_link = "https://www.transfermarkt.co.uk" + team_year_player_link
    soup = get_page_soup(player_link)

    # Grab the player's data
    table = soup.find('table', attrs={'class': 'auflistung'})
    table_rows = table.find_all('tr')

    player_data = {}

    # Extract the player's data
    for tr in table_rows:
        th = tr.find_all('th')
        td = tr.find_all('td')
        header = [th.text for th in th]
        data = [tr.text for tr in td]
        row = header + data

        # story key -> value for player data
        player_data[row[0][:-1].strip()] = row[1].strip()

    cleaned_player_data = clean_extract_player_data(player_link, player_data)

    return cleaned_player_data


def load_scrape_player_data(team_year_player_link):
    """Load player data from cache or scrape it, calculate player age for the year at season start and add it"""

    cache_filename = 'extracted_player_data.pickle'
    try:
        with open(cache_filename, 'rb') as handle:
            extracted_player_data_cache = pickle.load(handle)
    except FileNotFoundError:
        extracted_player_data_cache = {}

    if team_year_player_link in extracted_player_data_cache:
        player_data = extracted_player_data_cache[team_year_player_link]
    else:
        try:
            player_data = scrape_player_data(team_year_player_link)
            extracted_player_data_cache[team_year_player_link] = player_data
            with open(cache_filename, 'wb') as handle:
                pickle.dump(extracted_player_data_cache, handle, protocol=pickle.HIGHEST_PROTOCOL)
        except requests.exceptions.RequestException:
            player_data = None

    return player_data


def calculate_player_age(year, player_data):
    age = None
    if 'dob' in player_data:
        dob = datetime.strptime(player_data['dob'], '%d/%m/%Y')
        season_start_date = datetime(int(year), 10, 1, 0, 0)  # 1st October of the year
        age = int(year) - dob.year
        if season_start_date.month < dob.month or \
                (season_start_date.month == dob.month and season_start_date.day < dob.day):
            age -= 1

    return age


def scrape_player_market_value_history(profile_link):
    """Scrape the market values history for a player"""
    # just construct the player transfer value link directly from profile link
    value_link = profile_link.replace('profil', 'marktwertverlauf')
    value_link = f"https://www.transfermarkt.co.uk{value_link}"

    if value_link is not None:
        # Get the player's transfer value page
        soup = get_page_soup(value_link)

        # Strip the transfer data (from javascript block)
        found = None
        script_soups = soup.findAll('script')
        for script_iter in range(len(script_soups)):
            # iterate through different scripts, starting with last one (most often the match)
            script_index = -1 * (script_iter+1)
            script = str(script_soups[script_index])
            pattern = "'series':\[(.*)\]"
            extract = re.search(pattern, script)
            if extract and extract is not None:
                found = extract.group(1)
                break

        if found is None:
            return None

        # Do some magic to turn it into values and store it in a table
        raw_data = eval(found.replace("\'", "\""))
        transfer_value_rows = []
        for row in raw_data['data']:
            # just keep the market value and datetime of the market value  TODO: keep more?
            transfer_value_rows.append((row['y'], datetime.strptime(row['datum_mw'], '%b %d, %Y')))
            # TODO we actually have the club and age here, extract?
            """
            example data row:
            {  
                'y': 5400000,
                'verein': 'Watford FC',
                'age': 24,
                'mw': '£5.40m',
                'datum_mw': 'Dec 10, 2019',
                'x': 1575932400000,
                'marker': {'symbol': 'url(https://tmssl.akamaized.net/images/wappen/verysmall/1010.png?lm=1468103673)'}
            }
            """

        # ensure sorted by ascending datetime
        transfer_value_rows.sort(key=lambda x: x[1])

        return transfer_value_rows
    else:
        return None


def get_player_market_value_history(player_name, position, player_link):
    """Get player market value from scrape or cache"""
    cache_filename = 'player_transfer_value_cache.pickle'

    try:
        with open(cache_filename, 'rb') as handle:
            player_transfer_value_cache = pickle.load(handle)
    except FileNotFoundError:
        player_transfer_value_cache = {}

    # get player transfer value history (first check cache)
    key = f"{player_name} {position}"
    if key not in player_transfer_value_cache or player_transfer_value_cache[key] is None:
        # if not in cache, scrape it and cache it
        transfer_value_history = scrape_player_market_value_history(player_link)
        player_transfer_value_cache[key] = transfer_value_history

        with open(cache_filename, 'wb') as handle:
            pickle.dump(player_transfer_value_cache, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        transfer_value_history = player_transfer_value_cache[key]

    return transfer_value_history


def get_season_player_market_value(year, transfer_value_history):
    """From player market value history, extract the market value closest to the season start date for a year"""
    transfer_value_before_season_start = None
    transfer_value_before_season_start_date = None
    if transfer_value_history is not None:
        # get the transfer value for this year (value just before season start)
        season_start_date = datetime(int(year), 10, 1, 0, 0)
        for value, value_datetime in transfer_value_history:
            if value_datetime <= season_start_date:
                transfer_value_before_season_start = value
                transfer_value_before_season_start_date = value_datetime
            else:
                if transfer_value_before_season_start is None:
                    transfer_value_before_season_start = value
                    transfer_value_before_season_start_date = value_datetime
                break

        if transfer_value_before_season_start is None:
            transfer_value_before_season_start = transfer_value_history[0][0]  # take the earliest value for player
            transfer_value_before_season_start_date = transfer_value_history[0][1]

    return transfer_value_before_season_start, transfer_value_before_season_start_date


def add_player_market_value_for_year(year, player_link, player_data):
    """Add year's market value for player to player_data dict"""

    transfer_value_history = get_player_market_value_history(player_data['player_name'],
                                                             player_data['position'],
                                                             player_link)

    transfer_value_before_season_start, transfer_value_before_season_start_date = \
        get_season_player_market_value(year, transfer_value_history)

    player_data['transfer_value'] = transfer_value_before_season_start
    player_data['transfer_value_date'] = transfer_value_before_season_start_date

    if player_data['transfer_value_date'] is not None and type(player_data['transfer_value_date']) == datetime:
        player_data['transfer_value_date'] = player_data['transfer_value_date'].strftime('%d/%m/%Y')

    return player_data


def load_scrape_year_links(year):
    """For a year, scrape the team links and team player links (check cache first)"""
    try:
        with open('links_cache.pickle', 'rb') as handle:
            links_cache = pickle.load(handle)
    except FileNotFoundError:
        links_cache = {}

    year_team_links = None
    team_player_links = None
    if year in links_cache:
        if 'year_team_links' in links_cache[year]:
            year_team_links = links_cache[year]['year_team_links']

        if 'team_player_links' in links_cache[year]:
            team_player_links = links_cache[year]['team_player_links']

    pickle_updated = False
    if year_team_links is None:
        year_team_links = extract_team_links(year)
        if year not in links_cache:
            links_cache[year] = {}
        links_cache[year]['year_team_links'] = year_team_links
        pickle_updated = True

    if team_player_links is None:
        team_player_links = extract_team_player_links(year_team_links)
        if year not in links_cache:
            links_cache[year] = {}
        links_cache[year]['team_player_links'] = team_player_links
        pickle_updated = True

    if pickle_updated:
        with open('links_cache.pickle', 'wb') as handle:
            pickle.dump(links_cache, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return year_team_links, team_player_links


def main():
    """
        For each year:
            - get team links
            - get team player links
            - get player data
            - get player market value history
            - create a row of player attributes and market value from history as close to season start date for the year
    """
    years = [
        '2011',
        '2012',
        '2013',
        '2014',
        '2015',
        '2016',
        '2017',
        '2018',
        '2019',
        '2020',
    ]

    row_data = [
        ['Year',
         'Player Name',
         'Club',
         'Transfer Value',
         'Transfer Date',
         'Age',
         'DOB',
         'Height (cm)',
         'Foot',
         'Citizenship',
         'Position',
         'Player Link']
    ]
    for year in tqdm.tqdm(years):
        year_team_links, team_player_links = load_scrape_year_links(year)
        pprint(year)
        
        for team, player_links in team_player_links.items():
            for player_link in player_links:
                extracted_player_data = load_scrape_player_data(player_link)

                if extracted_player_data:
                    extracted_player_data = add_player_market_value_for_year(year,
                                                                             player_link,
                                                                             extracted_player_data)

                    row = [
                        year,
                        extracted_player_data['player_name'],
                        team,
                        extracted_player_data['transfer_value'],
                        extracted_player_data['transfer_value_date'],
                        calculate_player_age(year, extracted_player_data),
                        extracted_player_data['dob'],
                        extracted_player_data['height'],
                        extracted_player_data['foot'],
                        extracted_player_data['citizenship'],
                        extracted_player_data['position'],
                        player_link
                    ]
                    #pprint(row)
                    row_data.append(row)

    # output final CSV
    with open(f'extracted_transfer_data_{years[0]}_to_{years[-1]}.csv', 'w', encoding='utf8') as f:
        writer = csv.writer(f)
        writer.writerows(row_data)


def output_player_market_value_history():
    try:
        with open('player_transfer_value_cache.pickle', 'rb') as handle:
            player_transfer_value_cache = pickle.load(handle)
    except FileNotFoundError:
        player_transfer_value_cache = {}

    row_data = [
        ['Player Name Position',
         'Transfer Value',
         'Transfer Date']
    ]
    for player_position, value_history in player_transfer_value_cache.items():
        if value_history:
            for value, value_date in value_history:
                row = [
                    player_position,
                    value,
                    value_date.strftime('%d/%m/%Y')
                ]
                row_data.append(row)

    with open(f'player_market_value_history.csv', 'w', encoding='utf8') as f:
        writer = csv.writer(f)
        writer.writerows(row_data)


if __name__ == '__main__':
    main()

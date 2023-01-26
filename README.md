# CryptoTwitterSentiment

TwitterSentiment is a Python library to download crypto / nft tweets, analyze their sentiment and indicate the most
popular crypto project around. Can be used in other contexts as well

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements.

```bash
pip install -r requirements.txt
```
## Setup
Add your environmental configs, which can be found in EnvironmentalVariables.py

## Usage

See main.py for basic example of usage

- ScraperPipeline - full scraper pipeline 
- SentimentAnalysis - class to fetch texts from database, analyze their sentiment using multiple models and upload to postgres
- ScrapeTwitter - class to scrape and manipulate tweets and tweet-related data (user info etc.)
- SQLCreateTables - script to create postgres database  

## Todo
- Automatize daily scraper and daily email notifications of top NFT / crypto projects
- Add config.yml
- Figure out optimal sentiment aggregation and insights generation
- Sanitize code
- Make sentiment inference faster 
- Make automatic local postgres database creation for test purposes
- Scrape other sources (e.g. telegram, reddit)

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.


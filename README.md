# TradeScraper

This repo is for those interested in analyzing bilateral _goods_ trade data for all countries recognized by the United Nations and for all goods under the Harmonized System. Note that excellent alternative resources are available for direct trade data download, though obviously there will be constraints specific to each dataset. If you are interested in U.S. bilateral trade *only*, see Peter Schott's website where you can directly download .csv files (the site is frequently updated: https://faculty.som.yale.edu/peterschott/international-trade-data/).

The getHS6.py script implements the following:
1. Uses bulk API to scrape international goods trade data from UN Comtrade (https://comtrade.un.org/).
2. Cleans and matches country names with official international country codes.
3. Merges with international service trade data.
4. Creates one large dataset containing all of world's bilateral trade in goods and services, 2000-2014.

*Note*: Check frequently API updates at UN Comtrade. You may be able to avoid unnecessary coding!

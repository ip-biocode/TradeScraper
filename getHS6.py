# -*- coding: utf-8 -*-
"""
Goods Trade Data: HS6 As Reported
In Young Park
"""

import os, urlparse, requests, zipfile, io, glob, pycountry, csv
import pandas as pd

dir = os.getcwd()
os.listdir(dir)
path = '/Users/inyoungpark/Dropbox/BIGservice/data/raw_data'
os.chdir(path)



##
## Get data
##


## Bulk API Documentation: http://comtrade.un.org/data/doc/api/bulk/
# Example from documentation: http://comtrade.un.org/api/get/bulk/S/A/2014/251/EB02
# Note this URL downloads a zip file and saves a csv file
# Year 2014, with France as reporter (reporter code 251)

bulk_test_url = urlparse.urljoin('http://comtrade.un.org/api/get/bulk/', 'C/A/2000/152/HS?')
print(bulk_test_url) # verify we have correct URL
bulk_test_response = requests.get(bulk_test_url)
os.listdir(dir) # verify download in directory

# write to zip file, and then read as csv file
bulk_test_zip = zipfile.ZipFile(io.BytesIO(bulk_test_response.content))
extract = bulk_test_zip.extractall()

# extract desired columns and save as dataframe
headers = ["Year", "Trade Flow Code","Reporter","Reporter Code","Partner","Partner Code","Commodity","Commodity Code","Trade Value (US$)"]
headers


## Iterate over all years (2000-2014, inclusive) setting reporter = all
y = 2000
while y <= 2014:
	# construct url using each year
	year = str(y)
	url = 'http://comtrade.un.org/api/get/bulk/C/A/' + year + '/all/HS'
	response = requests.get(url)
	
	# write to zip file
	zippit = zipfile.ZipFile(io.BytesIO(response.content))
	zippit.extractall()
	
	# get name of the most recently downloaded csv file and read
	newest = max(glob.iglob('*.csv'), key = os.path.getctime)
	file = pd.read_csv(newest)

	# get desired columns from the file and write to csv file
	file_extract = file[headers]
	file_extract.to_csv(year + '_goods_hs6' + '.csv', sep = ',')
	
	# repeat for following year
	y = y + 1



##
## Clean data
##


## Aggregate 'year+serviceTrade.csv'
def get_merged_csv(flist):
    return pd.concat([pd.read_csv(f) for f in flist], ignore_index=True)

myfiles = os.path.join(path, '*_goods_hs6.csv')
mydata = get_merged_csv(glob.glob(myfiles))
data = mydata.drop('Unnamed: 0', 1)

# Reporters
# change 'Reporter' column to ISO codes
input_countries = data['Reporter'] #199 countries in dataset

countries = {}

# ISO-2 digit
for country in pycountry.countries:
	countries[country.name] = [country.alpha2]	
ISO2r = [countries.get(country, 'Unknown code') for country in input_countries]

# ISO-3 digit
for country in pycountry.countries:
	countries[country.name] = [country.alpha3]	
ISO3r = [countries.get(country, 'Unknown code') for country in input_countries]

iso2codes = [item[0].encode('ascii')  for item in ISO2r]
iso3codes = [item[0].encode('ascii')  for item in ISO3r]

# create columns to data frame
data['Reporter ISO2'] = iso2codes
data['Reporter ISO3'] = iso3codes
 
# Repeat same procedure for Partners
input_partners = data['Partner'] #199 countries in dataset

partners = {}
for country in pycountry.countries:
	partners[country.name] = [country.alpha2]	
ISO2p = [partners.get(country, 'Unknown code') for country in input_partners]

for country in pycountry.countries:
	partners[country.name] = [country.alpha3]	
ISO3p = [partners.get(country, 'Unknown code') for country in input_partners]

iso2codes_partner = [item[0].encode('ascii')  for item in ISO2p]
iso3codes_partner = [item[0].encode('ascii')  for item in ISO3p]
data['Partner ISO2'] = iso2codes_partner
data['Partner ISO3'] = iso3codes_partner

# reorder columns
cols = list(data.columns.values)
data = data[['Year',
 'Trade Flow Code',
 'Reporter', 'Reporter Code', 'Reporter ISO', 'Reporter ISO2', 'Reporter ISO3',
 'Partner', 'Partner Code', 'Partner ISO', 'Partner ISO2', 'Partner ISO3',
 'Commodity Code',
 'Commodity',
 'Trade Value (US$)']]

# 'U's generated in ISO codes...?
u = data.loc[data['Reporter ISO2'].str.match('U'), 'Reporter']
u.unique()

udic = {'World': ['WL', 'WLD'],
'Bolivia': ['BO', 'BOL'],
'Bosnia Herzegovina': ['BA', 'BIH'],
'Solomon Isds': ['SB', 'SLB'],
'EU25': ['NULL', 'NULL'],
'Cabo Verde': ['NULL','NULL'],
'Czech Rep.': ['CZ', 'CZE'],
'Central African Rep.': ['CF', 'CAF'], 
'Asia n.i.e.': ['NULL', 'NULL'],
'Dem. Rep. of the Congo': ['CD', 'COD'], 
'Dominican Rep.': ['DO','DOM'],
'Faeroe Isds': ['FO', 'FRO'], 
'State of Palestine': ['NULL', 'NULL'], 
'China, Hong Kong SAR': ['HK', 'HKG'], 
'Iran': ['IR', 'IRN'],
"C\xc3\xb4te d'Ivoire": ['CI', 'CIV'], 
"Lao People's Dem. Rep.": ['LA', 'LAO'],
'China, Macao SAR': ['MO', 'MAC'], 
'Rep. of Moldova': ['MD', 'MDA'], 
'Neth. Antilles': ['AN', 'ANT'],
'FS Micronesia': ['FM', 'FSM'], 
'Marshall Isds': ['MH', 'MHL'], 
'Saint Helena': ['SH', 'SHN'], 
'Syria': ['SY', 'SYR'], 
'Uganda': ['UG', 'UGA'],
'Ukraine': ['UA', 'UKR'], 
'TFYR of Macedonia': ['MK', 'MKD'], 
'United Rep. of Tanzania': ['TZ', 'TZA'],
'Uruguay': ['UY', 'URY'], 
'USA': ['US', 'USA'],
'Uzbekistan': ['UZ', 'UZB'], 
'Venezuela': ['VE', 'VEN'],
'Rep. of Korea': ['KR', 'KOR'],
'EU27': ['EU', 'EU2']}

#test
d = {'Bolivia': ['BO', 'BOL'],
'Bosnia Herzegovina': ['BA', 'BIH']}
for key, (value1, value2) in d.iteritems():
	print key, 'has ISO2 of', value1, 'and ISO3 of', value2


for k, (v1, v2) in udic.iteritems():
	data.loc[data.Reporter == k, 'Reporter ISO2'] = v1
	data.loc[data.Reporter == k, 'Reporter ISO3'] = v2
	data.loc[data.Partner == k, 'Partner ISO2'] = v1
	data.loc[data.Partner == k, 'Partner ISO3'] = v2
data[data['Partner'].str.contains('EU27')]
data[data['Partner'].str.contains('World')]

## write to csv file
data.to_csv('serviceTrade.csv', sep = ',')




####
## For each year + serviceTrade + .csv file, include ISO code
####

serv = pd.read_csv('serviceTrade.csv')
serv[1:4]


# Reporters
# change 'Reporter' column to ISO codes
input_countries = serv['Reporter'] #199 countries in dataset

countries = {}

# ISO numeric
for country in pycountry.countries:
	countries[country.name] = [country.numeric]	
ISOr = [countries.get(country, 'Unknown code') for country in input_countries]


# ISO-2 digit
for country in pycountry.countries:
	countries[country.name] = [country.alpha2]	
ISO2r = [countries.get(country, 'Unknown code') for country in input_countries]

# ISO-3 digit
for country in pycountry.countries:
	countries[country.name] = [country.alpha3]	
ISO3r = [countries.get(country, 'Unknown code') for country in input_countries]

isocodes = [item[0].encode('ascii')  for item in ISOr]
iso2codes = [item[0].encode('ascii')  for item in ISO2r]
iso3codes = [item[0].encode('ascii')  for item in ISO3r]

# create columns to data frame
serv['Reporter ISO'] = isocodes
serv['Reporter ISO2'] = iso2codes
serv['Reporter ISO3'] = iso3codes
 
# Repeat same procedure for Partners
input_partners = serv['Partner'] #199 countries in dataset

partners = {}

for country in pycountry.countries:
	countries[country.name] = [country.numeric]	
ISOp = [countries.get(country, 'Unknown code') for country in input_countries]

for country in pycountry.countries:
	partners[country.name] = [country.alpha2]	
ISO2p = [partners.get(country, 'Unknown code') for country in input_partners]

for country in pycountry.countries:
	partners[country.name] = [country.alpha3]	
ISO3p = [partners.get(country, 'Unknown code') for country in input_partners]

isocodes_partner = [item[0].encode('ascii')  for item in ISOp]
iso2codes_partner = [item[0].encode('ascii')  for item in ISO2p]
iso3codes_partner = [item[0].encode('ascii')  for item in ISO3p]
serv['Partner ISO'] = isocodes_partner
serv['Partner ISO2'] = iso2codes_partner
serv['Partner ISO3'] = iso3codes_partner

# reorder columns
cols = list(serv.columns.values)
serv = serv[['Year',
 'Trade Flow Code',
 'Reporter', 'Reporter Code', 'Reporter ISO', 'Reporter ISO2', 'Reporter ISO3',
 'Partner', 'Partner Code', 'Partner ISO', 'Partner ISO2', 'Partner ISO3',
 'Commodity Code',
 'Commodity',
 'Trade Value (US$)']]

# 'U's generated in ISO codes...?
u0 = serv.loc[serv['Reporter ISO'].str.match('U'), 'Reporter']
u0.unique()

udic2 = {'World': [0, 'WL', 'WLD'],
'Bolivia': [68, 'BO', 'BOL'],
'Bosnia Herzegovina': [70, 'BA', 'BIH'],
'Solomon Isds': [90, 'SB', 'SLB'],
'EU25': ['NULL', 'NULL', 'NULL'],
'Cabo Verde': ['NULL', 'NULL','NULL'],
'Czech Rep.': [203, 'CZ', 'CZE'],
'Central African Rep.': [140, 'CF', 'CAF'], 
'Asia n.i.e.': ['NULL','NULL', 'NULL'],
'Dem. Rep. of the Congo': [180, 'CD', 'COD'], 
'Dominican Rep.': [214, 'DO','DOM'],
'Faeroe Isds': [234, 'FO', 'FRO'], 
'State of Palestine': ['NULL','NULL', 'NULL'], 
'China, Hong Kong SAR': [344, 'HK', 'HKG'], 
'Iran': [364, 'IR', 'IRN'],
"C\xc3\xb4te d'Ivoire": [384, 'CI', 'CIV'], 
"Lao People's Dem. Rep.": [418, 'LA', 'LAO'],
'China, Macao SAR': [446, 'MO', 'MAC'], 
'Rep. of Moldova': [498, 'MD', 'MDA'], 
'Neth. Antilles': [530, 'AN', 'ANT'],
'FS Micronesia': [583, 'FM', 'FSM'], 
'Marshall Isds': [584, 'MH', 'MHL'], 
'Saint Helena': [654, 'SH', 'SHN'], 
'Syria': [760, 'SY', 'SYR'], 
'Uganda': [800, 'UG', 'UGA'],
'Ukraine': [804, 'UA', 'UKR'], 
'TFYR of Macedonia': [807, 'MK', 'MKD'], 
'United Rep. of Tanzania': [834, 'TZ', 'TZA'],
'Uruguay': [858, 'UY', 'URY'], 
'USA': [842, 'US', 'USA'],
'Uzbekistan': [860, 'UZ', 'UZB'], 
'Venezuela': [862, 'VE', 'VEN'],
'Rep. of Korea': [410, 'KR', 'KOR'],
'EU27': [97, 'EU', 'EU2']}

for k, (v1, v2, v3) in udic2.iteritems():
	serv.loc[serv.Reporter == k, 'Reporter ISO'] = v1
	serv.loc[serv.Reporter == k, 'Reporter ISO2'] = v2
	serv.loc[serv.Reporter == k, 'Reporter ISO3'] = v3
	serv.loc[serv.Partner == k, 'Partner ISO'] = v1
	serv.loc[serv.Partner == k, 'Partner ISO2'] = v2
	serv.loc[serv.Partner == k, 'Partner ISO3'] = v3
serv[serv['Partner'].str.contains('EU27')]
serv[serv['Partner'].str.contains('World')]

## write to csv file
serv.to_csv('serviceTrade.csv', sep = ',')



####################################
#### ISO code error correction
#####################################

## Read in ISO code file
iso_dat = pd.read_csv('ISOcode.csv')
list(iso_dat.columns.values)[0]
iso = iso_dat.drop(iso_dat.columns[[1,2,3,4,5,7,8]], axis=1)
list(iso)
iso = iso[['ISO3-digit Alpha', 'Country Code']]
iso.columns = ['alpha', 'num']
iso.to_csv('iso0817.csv', sep = ',')


## Create ISO code dictionary
reader = csv.reader(open('iso0817.csv'))
result = {}
for row in reader:
    key = row[1]
    if key in result:
        pass
    result[key] = row[2:]
print result


## Read in service trade data
serv_dat = pd.read_csv('serviceTrade0802.csv')
cols = list(serv_dat.columns.values)
cols


## Fill in Reporter ISO and Partner ISO 
for k, v in result.iteritems():
	serv_dat.loc[serv_dat['Reporter ISO3'] == k, 'Reporter ISO'] = v
	serv_dat.loc[serv_dat['Partner ISO3'] == k, 'Partner ISO'] = v


## Verify
serv_dat.loc[serv_dat['Reporter ISO3'] == 'KAZ', 'Reporter ISO']


## Write to csv file
serv_dat.to_csv('serviceTrade0817.csv', sep = ',')



























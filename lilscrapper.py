import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date, datetime
from utils.handy import *
from utils.bs4_utils import *
from unidecode import unidecode  # Import the unidecode function



#url_to_follow = "https://manati.mx/2021/07/15/lugares-en-puebla-para-ir-a-romancear-en-pareja-puebla/"
#url_to_follow = "https://tipsparatuviaje.com/lugares-romanticos-en-puebla/"
url_to_follow = "https://puebla.guiaoca.mx/es/contenido/7-lugares-romanticos-para-cenar-en-pareja-%E2%9D%A4%EF%B8%8F/"


def FollowLinkEchoJobs(url_to_follow: str) -> str:

	total_links = []
	total_titles = []
	total_descriptions = []
	total_timestamps = []
	rows = {"title": total_titles,
			"link": total_links,
			"description": total_descriptions,
			"timestamp": total_timestamps}

	# Make a request to the website
	r = requests.get(url_to_follow)
	r.content

	# Use the 'html.parser' to parse the page
	soup = BeautifulSoup(r.content, 'html.parser')

	#print(soup.prettify())

	# Find all h2 tags
	titles = soup.find_all("h2")
	links = soup.find_all("")
	descriptions = soup.find_all("")

	pattern = re.compile(r'^\d+\. ')
	for i in titles:
		title = i.text
		if title:
			if pattern.match(title):
				title = pattern.sub('', title)  # Assign the modified string back to title
				title = unidecode(title)  # Replace accents
				total_titles.append(title.lower())
		else:
			total_titles.append("NaN")

	# Define the regex pattern for links starting with "https://goo.gl/"
	pattern = re.compile(r'^https://goo\.gl/')
	for i in links:
		link = i.get("href")
		if link:
			if pattern.match(link):
				total_links.append(link)
		else:
			total_links.append("NaN")
	"""
	for i in links:
		link = i.text if i.text else "NaN"
		total_links.append(link)"""

	#Timestamps
	timestamp = datetime.now()

	for i in descriptions:
		description = i.text if i.text else "NaN"
		total_descriptions.append(description)

	# Ensure all lists have the same length
	max_length = max(len(total_titles), len(total_links), len(total_descriptions))

	total_titles.extend(["NaN"] * (max_length - len(total_titles)))
	total_links.extend(["NaN"] * (max_length - len(total_links)))
	total_descriptions.extend(["NaN"] * (max_length - len(total_descriptions)))
	total_timestamps.extend([timestamp] * max_length)


	rows = {'title':total_titles, 'link':total_links, 'description': total_descriptions, 'timestamp': total_timestamps}

	return rows

#data = FollowLinkEchoJobs(url_to_follow)

#-> DF
#df = pd.DataFrame(data)

#print(df)

#to_local_test(df)
#to_local_postgre(df)

def OtherLilOne(url_to_follow: str) -> str:

	total_links = []
	total_titles = []
	total_descriptions = []
	total_timestamps = []
	rows = {"title": total_titles,
			"link": total_links,
			"description": total_descriptions,
			"timestamp": total_timestamps}

	# Make a request to the website
	r = requests.get(url_to_follow)
	r.content

	# Use the 'html.parser' to parse the page
	soup = BeautifulSoup(r.content, 'html.parser')

	#print(soup.prettify())

	# Find all h2 tags
	titles = soup.select(".title.title--serif.title--left.title--padded")
	links = soup.select(".button.button--default.local")
	descriptions = soup.select(".text.text--serif.text--left")


	for i in titles:
		title = i.text
		if title:
			title = unidecode(title)  # Replace accents
			total_titles.append(title.lower())
		else:
			total_titles.append("NaN")

	# Define the regex pattern for links starting with "https://goo.gl/"
	for i in links:
		print(i)
		link = i.get("href")
		if link:
			total_links.append(link)
		else:
			total_links.append("NaN")
	"""
	for i in links:
		link = i.text if i.text else "NaN"
		total_links.append(link)"""

	#Timestamps
	timestamp = datetime.now()

	for i in descriptions[1:]:
		description = i.text if i.text else "NaN"
		total_descriptions.append(description)

	# Ensure all lists have the same length
	max_length = max(len(total_titles), len(total_links), len(total_descriptions))

	total_titles.extend(["NaN"] * (max_length - len(total_titles)))
	total_links.extend(["NaN"] * (max_length - len(total_links)))
	total_descriptions.extend(["NaN"] * (max_length - len(total_descriptions)))
	total_timestamps.extend([timestamp] * max_length)


	rows = {'title':total_titles, 'link':total_links, 'description': total_descriptions, 'timestamp': total_timestamps}

	return rows

data = OtherLilOne(url_to_follow)

df = pd.DataFrame(data)

#print(df)

#to_local_test(df)
to_local_postgre(df)

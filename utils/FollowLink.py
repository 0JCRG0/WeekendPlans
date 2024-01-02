import requests
import bs4
import traceback
import re
import aiohttp
import time
from asyncio import TimeoutError
from concurrent.futures import ThreadPoolExecutor
import logging
#from handy import LoggingMasterCrawler
import os
from traceback import format_exc
import random
from traceback import format_exc

async def async_follow_link(session: aiohttp.ClientSession, followed_link, description_final, inner_link_tag, default):

	async with session.get(followed_link) as link_res:
		if link_res.status == 200:
			print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
			link_text = await link_res.text()
			link_soup = bs4.BeautifulSoup(link_text, 'html.parser')
			description_tag = link_soup.select_one(inner_link_tag)
			if description_tag:
				description_final = description_tag.text
				return description_final
			else:
				description_final = 'NaN'
				return description_final
		else:
			print(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""", "\n")
			logging.warning(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""")
			description_final = default
			return description_final


async def AsyncFollowLinkEchoJobs(session: aiohttp.ClientSession, url_to_follow: str, selector: str, default: str) -> str:

	async with session.get(url_to_follow) as r:
		try:
			if r.status == 200:
				
				print(f"""CONNECTION ESTABLISHED ON {url_to_follow}. USING FollowLinkEchoJobs()\n""")

				request = await r.text()

				soup = bs4.BeautifulSoup(request, 'html.parser')

				# Find the 'div' tag with the class "job-detail mb-4"
				div_tag = soup.find('div', {'class': selector})

				if div_tag:
					description_text = div_tag.get_text()
					return description_text
				else:
					logging.warning(f"Setting description to default at AsyncFollowLinkEchoJobs().\nFollowing {url_to_follow}\n")
					return default
			else:
				logging.warning(f"""CONNECTION FAILED ON {url_to_follow}. STATUS CODE: "{r.status}". Setting description to default.""")
				return default
		except Exception as e:
			logging.warning(f"Exception at AsyncFollowLinkEchoJobs(). Following {url_to_follow}\n {e}")
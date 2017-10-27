import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
import MySQLdb

conn = MySQLdb.connect(host= "localhost",
                  user="user",
                  passwd="password")
x = conn.cursor()
sql = """CREATE DATABASE IF NOT EXISTS Reuters"""
x.execute(sql)
sql = """USE Reuters"""
x.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS Links(link VARCHAR(255) UNIQUE, downloaded BOOLEAN NOT NULL)"""
x.execute(sql)
ROOT = "http://www.reuters.com/resources/archive/us/"
FILETYPE = ".html"
now = datetime.datetime.now().strftime("%Y%m%d")
now = datetime.datetime.strptime(now, "%Y%m%d")
print(now)
start = datetime.datetime.strptime("20070101", "%Y%m%d")
while start < now:
	print(start)
	start = start + datetime.timedelta(days=1)
	html = urlopen(ROOT+start.strftime("%Y%m%d")+FILETYPE).read().decode('utf-8')
	soup = BeautifulSoup(html, 'lxml')
	for div in soup.findAll("div", { "class" : "headlineMed" }):
		for link in div.find_all('a'):
			if "video" in link['href']:
				pass
			else:
				print(link['href'])
				try:
					x.execute("""INSERT INTO Links VALUES (%s,%s)""",(link['href'], 0))
					conn.commit()
				except Exception as e:
					print(e)
					conn.rollback()
conn.close()

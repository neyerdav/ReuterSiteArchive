from bs4 import BeautifulSoup
from urllib.request import urlopen
import MySQLdb

conn = MySQLdb.connect(host= "localhost",
                  user="user",
                  passwd="password")
x = conn.cursor()
conn.set_character_set('utf8')
x.execute('SET NAMES utf8;')
x.execute('SET CHARACTER SET utf8;')
x.execute('SET character_set_connection=utf8;')

sql = """CREATE DATABASE IF NOT EXISTS Reuters"""
x.execute(sql)
sql = """USE Reuters"""
x.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS Links(link VARCHAR(255) UNIQUE, downloaded BOOLEAN NOT NULL)"""
x.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS Articles(headline VARCHAR(255) UNIQUE, article TEXT, date VARCHAR(255), time VARCHAR(255), author VARCHAR(255), category VARCHAR(255))"""
x.execute(sql)

while True:
	try:
		sql = """Select * from Links WHERE downloaded = 0 LIMIT 1"""
		x.execute(sql)
		for link, downlaoded in x:
			print(link)
			html = urlopen(link).read().decode('utf-8')
			sql = """UPDATE Links SET downloaded = 1 WHERE link = "{0}";""".format(link)
			x.execute(sql)
		soup = BeautifulSoup(html, 'lxml')
		try:
			author = soup.find("p", { "class" : "BylineBar_byline_31BCV" }).text
		except:
			author = "na"
		try:
			category = soup.find("div", { "class" : "ArticleHeader_channel_4KD-f" }).text[1:]
		except:
			category = "na"
		try:
			headline = soup.find("h1", { "class" : "ArticleHeader_headline_2zdFM" }).text
			article = soup.find("div", { "class" : "ArticleBody_body_2ECha" }).text
			DateAndTime = soup.find("div", { "class" : "ArticleHeader_date_V9eGk" }).text
			pos = DateAndTime.find("/")
			date = DateAndTime[:pos-1]
			pos1 = DateAndTime[pos+3:].find("/")
			time = DateAndTime[pos+3:pos+3+pos1-1]
			x.execute("""INSERT INTO Articles VALUES (%s, %s, %s, %s, %s, %s)""", (headline, article, date, time, author, category))
			conn.commit()
		except Exception as e:
			print(e)
			conn.rollback()
	except Exception as e:
		print(e)





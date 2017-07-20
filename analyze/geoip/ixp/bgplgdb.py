import urllib
import os
import signal
import HTMLParser
import sys

#Parser for BGP Looking Glass Database.
class BGPLGDBParser(HTMLParser.HTMLParser):
	def __init__(self):
		HTMLParser.HTMLParser.__init__(self)
		self.lines=[]
		self.line=[]
		self.line_cnt=0
		self.is_td=False
		self.is_body=False
		self.is_script=False
		self.td=""
	
	def handle_starttag(self, tag, attrs):
		if (not self.is_body and tag == "tbody"):
			self.is_body = True
		
		if (self.is_body and tag == "td" and self.line_cnt >= 1):
			self.is_td = True
		
		if (self.is_body and tag == "script"):
			self.is_script = True
	
	def handle_endtag(self, tag):
		if (self.is_body and tag == "tr"):
			if (self.line_cnt >= 2):
				ixp=self.line[0]
				address=self.line[1]
				city=self.line[2]
				country=self.line[3]
				email=self.line[4]
				url=self.line[5]
				self.lines.append("%s|%s|%s|%s|%s|%s" % (ixp, address, city, country, email, url))
				self.line=[]
			self.line_cnt+=1
			
		if (self.is_td and tag == "td"):
			self.is_td = False
			self.line.append(self.td)
			self.td=""
		
		if (self.is_td and tag == "script"):
			self.is_script = False
		
	def handle_data(self, data):
		if (self.is_td and not self.is_script):
			self.td+=data

def parse_ixp_offline():
	file_list=["ixp-database.html", "ixp-database-2.html"]
	for f in file_list:
		parser = BGPLGDBParser()
		parser.feed(open(f,'rb').read())
		for l in parser.lines:
			print l

def parse_ixp():
	url_list=["http://www.bgplookingglass.com/ixp-database", "http://www.bgplookingglass.com/ixp-database-2"]
	for u in url_list:
		parser = BGPLGDBParser()
		parser.feed(urllib.urlopen(u).read())
		for l in parser.lines:
			print l

def main(argv):
	#parse_ixp()
	parse_ixp_offline() #email field not available in webpage source code

if __name__ == "__main__":
	main(sys.argv)

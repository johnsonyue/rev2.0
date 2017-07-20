import geoip2.database
import IP2Location
import lookup
import qqwry
import os

class geoip_helper():
	def __init__(self,source_list=["mmdb"]):
		self.lookup = None
		self.qqwry= None
		self.reader= None
		self.database = None
		#bgp.
		if "bgp" in source_list:
			self.lookup = lookup.lookup()
		
		#czdb.
		if "czdb" in source_list:
			self.czdb_path = "qqwry.dat"
			if not os.path.exists(self.czdb_path):
				qqwry.update_db(self.czdb_path)
			self.qqwry = qqwry.QQWry(self.czdb_path)
		
		#mmdb.
		if "mmdb" in source_list:
			self.reader = geoip2.database.Reader('GeoLite2-City.mmdb')
		
		#ip2location:
		if "ip2location" in source_list:
			self.database = IP2Location.IP2Location()
			self.database.open("IP2LOCATION-LITE-DB11.BIN")
	
	def query(self, ip):
		bgp = ""
		mmdb = ""
		czdb = ""
		ip2location = ""
		if self.lookup != None:
			bgp = self.query_from_bgp(ip)
		if self.reader != None:
			mmdb = self.query_from_mmdb(ip)
		if self.qqwry != None:
			czdb = self.query_from_czdb(ip)
		if self.database != None:
			ip2location = self.query_from_czdb(ip)
		return {"bgp":bgp, "mmdb":mmdb, "czdb":czdb, "ip2location":ip2location}
	
	def query_from_bgp(self, ip):
		asn = self.lookup.get_asn_from_pfx(ip)
		if (asn != None):
			asn_cc = self.lookup.get_cc_from_asn(asn)
		else:
			asn = "*"
			asn_cc = "*"

		return {"asn":asn, "country":asn_cc}
	
	def query_from_mmdb(self, ip):
		try:
			response = self.reader.city(ip)
			country = response.country.iso_code 
			if (country == None):
				country = "*"
			city = response.city.name
			if (city == None):
				city = "*"
			return {"longitude":response.location.longitude, "latitude":response.location.latitude, "country":country, "city":city}
		except geoip2.errors.AddressNotFoundError:
			return {"longitude":"*", "latitude":"*", "country":"*", "city":"*"}
	
	def query_from_czdb(self, ip):
		try:
			country, area = self.qqwry.query(ip)
			return {"country":country, "area":area}
		except:
			return {"country":"*", "area":"*"}
	
	def query_from_ip2location(self, ip):
		try:
			rec = self.database.get_all(ip)
			
			if rec is not None:
				return {"country":rec.country_short, "city":rec.city, "latitude":rec.latitude, "longitude":rec.longitude}
		except:
				return {"country":"*", "city":"*", "latitude":"*", "longitude":"*"}
			

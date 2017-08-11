import json
import sys
from neo4j.v1 import GraphDatabase, basic_auth

from geoip import geoip

class db():
	def __init__(self, config):
		self.config = config
		address = config["db"]["address"]
		port = config["db"]["port"]
		username = config["db"]["username"]
		password = config["db"]["password"]

		self.driver = GraphDatabase.driver("bolt://%s:%s"%(address,port), auth=basic_auth(username, password))
	
	def add_annotation(self, csv_file_name, output_csv_name):
		sys.stderr.write("adding annotation ... ")
		sys.stderr.flush()
		lines = open(csv_file_name,'r').readlines()
	
		cypher_str = ""
		geo_dict = {}
		helper = geoip.geoip_helper()
		for line in lines[1:]:
			fields = line.strip('\n').split(',')
			ingress = fields[0]
			outgress = fields[1]
	
			if not geo_dict.has_key(ingress):
				bgp = helper.query_asn_from_bgp(ingress)
				geo = helper.query(ingress)
				geo_dict[ingress] = {"asn":bgp["asn"], "country":geo["mmdb"]["country"]}

			if not geo_dict.has_key(outgress):
				bgp = helper.query_asn_from_bgp(outgress)
				geo = helper.query(outgress)
				geo_dict[outgress] = {"asn":bgp["asn"], "country":geo["mmdb"]["country"]}
			
		fp = open(output_csv_name,'wb')
		fp.write("ingress,outgress,delay,connected,in_asn,in_country,out_asn,out_country\n")
		for line in lines[1:]:
			fields = line.strip('\n').split(',')
			ingress = fields[0]
			outgress = fields[1]
			delay = fields[2]
			connected= fields[3]
			
			in_asn = geo_dict[ingress]["asn"]
			in_country = geo_dict[ingress]["country"]
			out_asn = geo_dict[outgress]["asn"]
			out_country = geo_dict[outgress]["country"]
			fp.write(str(ingress) + "," + str(outgress) + "," + str(delay) + "," + str(connected) \
			+ "," + str(in_asn) + "," + str(in_country) + "," + str(out_asn) + "," + str(out_country) + "\n")
		
		fp.close()

		sys.stderr.write("done.\n")
		sys.stderr.flush()

	def import_from_csv(self, csv_file_name):
		session = self.driver.session()
		helper = geoip.geoip_helper()
		
		sys.stderr.write("importing from csv ... ")
		sys.stderr.flush()
				
		try:
			session.run(" \
				LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
				MERGE (in:node {ip:line.ingress, asn:line.in_asn, country:line.in_country}) \
				MERGE (out:node {ip:line.outgress, asn:line.out_asn, country:line.out_country}) \
				MERGE (in)-[e:edge {delay:line.delay, type:line.connected}]->(out) \
			" % (csv_file_name))
		except Exception, ex:
			sys.stderr.write("\n" + str(ex) + "\n")
			session.close()
			exit(-1)

		sys.stderr.write("done.\n")
		sys.stderr.flush()

		session.close()

	def drop_db(self):
		session = self.driver.session()
		
		sys.stderr.write("dropping db ... ")
		sys.stderr.flush()
		try:
			session.run( " \
				MATCH ()-[r]->() delete r\
				")
			session.run( " \
				MATCH (n) delete n\
				")
		except Exception, ex:
			sys.stderr.write("\n" + str(ex) + "\n")
			session.close()
			exit(-1)

		sys.stderr.write("done.\n")
		session.close()

config = json.loads(open("config.json").read())
db = db(config)
db.drop_db()
db.add_annotation("/var/lib/neo4j/import/test.csv","/var/lib/neo4j/import/data.csv")
db.import_from_csv("data.csv")

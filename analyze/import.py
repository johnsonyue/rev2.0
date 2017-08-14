import json
import sys
import getopt
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
				MERGE (in:node {ip:line.ingress}) \
				ON CREATE set in.ip = line.ingress, in.asn = line.in_asn, in.country = line.in_country \
				MERGE (out:node {ip:line.outgress}) \
				ON CREATE set out.ip = line.outgress, out.asn = line.out_asn, out.country = line.out_country \
				MERGE (in)-[e:edge]->(out) \
				ON CREATE set e.delay = line.delay, e.type = line.connected \
			" % (csv_file_name))
			#ON CREATE set e.delay = line.delay, e.type = line.connected \
			#ON MATCH set e.delay = e.delay, e.type = e.type \
		except Exception, ex:
			sys.stderr.write("\n" + str(ex) + "\n")
			session.close()
			exit(-1)

		sys.stderr.write("done.\n")
		sys.stderr.flush()

		session.close()
	
	def add_router_annotation(self, csv_file_name, node_csv_file_name, output_csv_name, output_node_csv_name):
		helper = geoip.geoip_helper()
		#node csv file
		fp = open(node_csv_file_name, 'rb')
		fpo = open(output_node_csv_name, 'wb')
		fpo.write("node_id,ip,asn,country\n")
		for line in fp.readlines():
			if line[0] == "#":
				continue
			fields = line.strip(' \n').split(":  ")
			node_id = fields[0]
			ip_str = fields[1]
			ip_list = ip_str.split(' ')

			asn_str = ""
			country_str = ""
			for ip in ip_list:
				asn = helper.query_asn_from_bgp(ip)["asn"]
				asn_str += str(asn) + " "
				country = helper.query(ip)["mmdb"]["country"]
				country_str += str(country) + " "
			asn_str = asn_str.strip(" ")
			country_str = country_str.strip(" ")

			fpo.write( str(node_id) + "," + str(ip_str) + "," + str(asn_str) + "," + str(country_str) + "\n" )
		fp.close()
		
		#link csv file
		fp = open(csv_file_name, 'rb')
		fpo2 = open(output_csv_name, 'wb')
		fpo2.write("a_id,b_id,a_ip,b_ip\n")
		for line in fp.readlines():
			if line[0] == "#":
				continue
			fields = line.strip(' \n').split(":  ")
			link_id = fields[0]
			node_str = fields[1]
			node_list = node_str.split(' ')
			
			if len(node_list) == 2:
				a_list = node_list[0].split(':')
				a_id = a_list[0]
				a_ip = "*" if len(a_list) == 1 else a_list[1]

				b_list = node_list[1].split(':')
				b_id = b_list[0]
				b_ip = "*" if len(b_list) == 1 else b_list[1]
				fpo2.write( str(a_id) + "," + str(b_id) + "," + str(a_ip) + "," + str(b_ip) + "\n" )
			else:
				fpo.write( str(link_id) + ",*,*,*\n" ) #treat link as a dummy node.
				for n in node_list:
					n_list = node_list[0].split(':')
					n_id = n_list[0]
					n_ip = "*" if len(n_list) == 1 else n_list[1]
					
					fpo2.write( str(n_id) + "," + str(link_id) + "," + str(n_ip) + ",*" + "\n" )
		
		fp.close()
		fpo.close()
		fpo2.close()
	
	def import_router_from_csv(self, csv_file_name, node_csv_file_name):
		session = self.driver.session()
		helper = geoip.geoip_helper()
		
		sys.stderr.write("importing router nodes from csv ... ")
		sys.stderr.flush()
				
		try:
			session.run(" \
				LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
				MERGE (n:router {node_id:line.node_id}) \
				ON CREATE SET n.node_id = line.node_id, n.ip = line.ip, n.asn = line.asn, n.country = line.country \
			" % (node_csv_file_name))
		except Exception, ex:
			sys.stderr.write("\n" + str(ex) + "\n")
			session.close()
			exit(-1)

		sys.stderr.write("done.\n")
		sys.stderr.flush()

		#import links
		sys.stderr.write("importing router links from csv ... ")
		sys.stderr.flush()

		try:
			session.run(" \
				LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
				MERGE (a:router {node_id:line.a_id})-[r:link]-(b:router {node_id:line.b_id}) \
				ON CREATE SET r.a_ip = line.a_ip, r.b_ip = line.b_ip \
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
	
	def create_index(self):
		session = self.driver.session()
		
		sys.stderr.write("creating db ... ")
		sys.stderr.flush()
		try:
			session.run( " \
				CREATE INDEX ON :node(ip) \
				")
		except Exception, ex:
			sys.stderr.write("\n" + str(ex) + "\n")
			session.close()
			exit(-1)

		sys.stderr.write("done.\n")
		session.close()

#config = json.loads(open("config.json").read())
#db = db(config)
#db.drop_db()
#db.add_annotation("/var/lib/neo4j/import/test.csv","/var/lib/neo4j/import/data.csv")
#db.import_from_csv("data.csv")

def usage():
	print "import -f <$csv_file_name> [-d] [-t <$import_type>]"

def main(argv):
	if (len(argv) <= 1):
		usage()
		exit()
	
	try:
		opts, args = getopt.getopt(argv[1:], "f:dit:n:h")
	except getopt.GetoptError as err:
		print str(err)
		usage()
		exit(2)

	csv_file_name = ""
	node_csv_file_name = ""
	drop_db = False
	create_index  = False
	
	import_type = "ip"

	for o,a in opts:
		if o == "-h":
			usage()
			exit()
		elif o == "-f":
			csv_file_name = a
		elif o == "-n":
			node_csv_file_name = a
		elif o == "-d":
			drop_db = True
		elif o == "-i":
			create_index = True
		elif o == "-t":
			import_type = a
	
	if not drop_db and not create_index and csv_file_name == "":
		usage()
		exit(2)
	
	if not import_type in ["ip", "router"]:
		usage()
		exit(2)

	if import_type == "router" and node_csv_file_name == "":
		usage()
		exit()

	config = json.loads(open("config.json").read())
	db_helper = db(config)
	if drop_db:
		db_helper.drop_db()
		exit()
	if create_index:
		db_helper.create_index()
		exit()
	
	if import_type == "ip":
		db_helper.add_annotation(csv_file_name,"/var/lib/neo4j/import/temp.csv")
		db_helper.import_from_csv("temp.csv")
	elif import_type == "router":
		db_helper.add_router_annotation(csv_file_name, node_csv_file_name,"/var/lib/neo4j/import/router_temp.csv", "/var/lib/neo4j/import/router_temp_node.csv")
		db_helper.import_router_from_csv("router_temp.csv","router_temp_node.csv")

if __name__ == "__main__":
	main(sys.argv)

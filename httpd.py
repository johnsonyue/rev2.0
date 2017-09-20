import json
import time
import subprocess
import BaseHTTPServer
import cgi
from SocketServer import ThreadingMixIn

class Server(BaseHTTPServer.HTTPServer):
	def __init__(self, (HOST_NAME, PORT_NUMBER), handler, config):
		BaseHTTPServer.HTTPServer.__init__(self, (HOST_NAME, PORT_NUMBER), handler)
		self.config = config
class ThreadedHTTPServer(ThreadingMixIn, Server):
	 def __init__(self, (HOST_NAME, PORT_NUMBER), handler, config):
		 Server.__init__(self, (HOST_NAME, PORT_NUMBER), handler, config)
	
class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
	def do_HEAD(self):
		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.end_headers()
	def do_GET(self):
		self.send_response(404)
		self.end_headers()
			
	def do_POST(self):
		config = self.server.config
		post = cgi.FieldStorage(
			fp=self.rfile, 
			headers=self.headers,
			environ={'REQUEST_METHOD':'POST',
			'CONTENT_TYPE':self.headers['Content-Type'],
		})
		if post.has_key("ip_list"):
			with open("temp_ip_list",'wb') as fp:
				fp.write(post["ip_list"].value)
				fp.close()
			p = subprocess.Popen(["./json.sh","temp_ip_list"], stdout=subprocess.PIPE)

			result_list = []
			for r in p.stdout.readlines():
				fields = r.strip('\n').split(' ')
				temp_dict = {}
				tr_dict = {}
				tr_dict["is_arrived"] = "false" if fields[1] == 'N' else "true"
				tr_dict["last_ip"] = fields[2]
				tr_dict["last_ttl"] = fields[3]
				tr_dict["timestamp"] = fields[4]
				temp_dict["ip"] = fields[0]
				temp_dict["traceroute"] = tr_dict
				result_list.append(temp_dict)
			
			self.send_response(200)
			self.end_headers()
			self.wfile.write(json.dumps(result_list))
		else:
			self.send_response(505)
			self.end_headers()

if __name__ == '__main__':
	config = json.loads(open("json.config").read())
	HOST_NAME = config["app"]["host_name"]
	PORT_NUMBER = config["app"]["port_number"]
	httpd = ThreadedHTTPServer( (HOST_NAME, PORT_NUMBER), Handler, config )
	print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
	try:
		httpd.serve_forever()
	except KeyboardInterrupt:
		pass
	httpd.server_close()
	print time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)

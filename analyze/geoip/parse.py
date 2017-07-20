import sys

#helper functions.
def ip_str2dec(ip):
	ip_sections = ip.split('.')
	ip_dec = map( lambda x:int(x), ip_sections )
	
	return ip_dec

def ip_cmp(ip_a, ip_b):
	ip_a_dec = ip_str2dec(ip_a)
	ip_b_dec = ip_str2dec(ip_b)
	for i in range(len(ip_a_dec)):
		if(ip_a_dec[i] == ip_b_dec[i]):
			continue
		return ip_a_dec[i] - ip_b_dec[i]

	return 0

def cmp_func(tuple_a, tuple_b):
	return ip_cmp(tuple_a[0].split('/')[0], tuple_b[0].split('/')[0])
	
def dict2list(pfx2asn):
	pfx2asn_list = []
	for k in pfx2asn.keys():
		org_asn_list = pfx2asn[k]
		org_asn_str = org_asn_list[0]
		for i in range(1,len(org_asn_list)):
			org_asn_str = org_asn_str+"_"+org_asn_list[i]
		pfx2asn_list.append((k,org_asn_str))
	
	return pfx2asn_list
	
#get target pfx list from asn list and bgp dump.
def get_pfx2asn_file(file_name):
	pfx2asn = {}
	while True:
		try:
			line=raw_input()
		except:
			break
		sections = line.split('|')
		pfx = sections[5]
		org = sections[6].split(' ')[-1]
		if (pfx != "0.0.0.0/0"):
			if (not pfx2asn.has_key(pfx)):
				pfx2asn[pfx] = [org]
			else:
				is_included = False
				for a in pfx2asn[pfx]:
					if (a == org):
						is_included = True
						break
				if not is_included:
					pfx2asn[pfx].append(org)
	
	pfx2asn_list = dict2list(pfx2asn)
	sorted_pfx2asn = sorted(pfx2asn_list, cmp=cmp_func)
	
	fp = open(file_name, 'wb')
	for i in range(len(sorted_pfx2asn)):
		tup = sorted_pfx2asn[i]
		ip = tup[0].split('/')[0]
		mask = tup[0].split('/')[1]
		asn = tup[1]
		fp.write(ip+"|"+mask+"|"+asn+"\n")
	fp.close()

def usage():
	print "python target.py file_name"
	print "e.g. python target.py pfx2asn"

def main(argv):
	if len(argv) < 2:
		usage()
		exit()
	file_name = argv[1]
	get_pfx2asn_file(file_name)

if __name__ == "__main__":
	main(sys.argv)

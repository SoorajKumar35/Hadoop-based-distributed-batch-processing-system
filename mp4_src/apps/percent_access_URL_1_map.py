import os
import sys
from urlparse import urlparse
import string

def percent_access_URL_1(value):

	output = []
	for line in value:
		try:
			table = string.maketrans("", "")
			url = line.split(" ")[6]
			domain = urlparse(url).netloc.translate(table, "/")
			output.append((url, 1))
		except IndexError:
			continue
	return output

# if __name__ == '__main__':
#     value = sys.argv[1:]
#     print(percent_access_URL_1(value))

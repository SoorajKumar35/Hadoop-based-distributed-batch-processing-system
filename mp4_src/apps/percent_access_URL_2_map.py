import os
import sys
import string

def percent_access_URL_2(value):

	output = []
	for line in value:
		output.append((1, line))
	return output

# if __name__ == '__main__':
#     value = sys.argv[1:]
#     print(percent_access_URL_2(value))

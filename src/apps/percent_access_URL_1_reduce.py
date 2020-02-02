import os
import sys
import string
import operator
import functools

def percent_access_URL_1(key, value):
	return functools.reduce(operator.add, value)

# if __name__ == '__main__':
#     key, value = sys.argv[1], sys.argv[2:]
#     value = [int(v) for v in value]
#     print(percent_access_URL_1(key, value))

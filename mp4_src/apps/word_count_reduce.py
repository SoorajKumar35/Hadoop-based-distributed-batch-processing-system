import os
import sys
import string
import operator
import functools

def word_count(key, value):
	s = 0
	for v in value:
		s += int(v)
	return s

# if __name__ == '__main__':
#     key, value = sys.argv[1], sys.argv[2:]
#     value = [int(v) for v in value]
#     print(percent_access_URL_1(key, value))

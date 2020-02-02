import os
import sys
import string
import operator
import functools

def percent_access_URL_2(key, value):
	value = [v.split(" ") for v in value]
	value = [v for v in value if v[0]]
	total_accesses = functools.reduce(operator.add, [float(v[1]) for v in value if v[1] != " "])
	percent_accesses = map(lambda x: (x[0], float(float(x[1])/float(total_accesses))), value)
	return percent_accesses

# if __name__ == '__main__':
# 	key, value = sys.argv[1], sys.argv[2:]
# 	value = [int(v) for v in value]
# 	print(percent_access_URL_2(key, value))

# if __name__ == '__main__':
# 	key, value = sys.argv[1], sys.argv[2:]
# 	value = [int(v) for v in value]
# 	print(percent_access_URL_2(key, value))
	
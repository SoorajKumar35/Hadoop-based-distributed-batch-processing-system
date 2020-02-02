import os
import sys
import string

def word_count(value):

	output = []
	for line in value:
		# print("Line: ", line)
		for word in line.split(" "):
			table = string.maketrans("", "")
			word = word.translate(table, string.punctuation)
			# print("word: ", word)
			if word:
				output.append((word, 1))
	return output

# if __name__ == '__main__':
#     value = sys.argv[1:]
#     print(word_count(value))

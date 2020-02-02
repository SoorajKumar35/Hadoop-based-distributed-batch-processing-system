
import os
import itertools
import sys
import string
import socket
import functools
import collections
import subprocess
import operator

class MapTask(object):
	"""
	Class that defines the Map functionality of the MapleJuice System
	"""

	def __init__(self, shard_dir=None, application=None, prefix=None):
		"""
		Initialize the Map function that handles the map phase for multiple applications
		"""	

		encoding = None
		if application == 'purl1': encoding = 'latin-1' 
		else: encoding = 'utf-8'
		if shard_dir:
			output = self.produce_output_tuples(shard_dir, encoding, application=application)	
			self.write_output(output, prefix=prefix, output_dir='inter_pairs')
		else:
			print("Please provide the directory where the shards are stored.")


	def produce_output_tuples(self,shard_dir=None, encoding=None, application=None):

		shard_output = [] 
		shard_files_list = [i for i in os.walk(shard_dir)]
		shard_files = shard_files_list[0][2]
		for i, shard in enumerate(shard_files):

			# Read ten lines per shard and generate intermediate key-value pairs
			with open(os.path.join(shard_dir, shard), 'r', encoding=encoding) as shard_fd:
				while True:				
					ten_lines = itertools.islice(shard_fd, 10)	
					value = [line.strip("\n").strip('\n') for line in ten_lines if line[0] != '#']
					if not value:
						break
					# print(value)
					if application == 'word_count':
						shard_output += self.word_count(value)
					elif application == 'purl1':
						shard_output += self.percent_access_URL_1(value)
					elif application == 'purl2':
						shard_output += self.percent_access_URL_2(value)
					else:
						print("Please enter a correct application")
						return
				shard_fd.close()
		return shard_output

	def write_output(self, shard_keyval_pairs=None, prefix=None, output_dir=None):
	
		if not os.path.exists(output_dir):
			os.makedirs(output_dir)

	
		key_values_dict = collections.defaultdict(list)
		if shard_keyval_pairs:
			functools.reduce(lambda self, pair: key_values_dict[pair[0]].append(pair[1]), shard_keyval_pairs)
		if prefix and output_dir:	
			for key, value in key_values_dict.items():
				with open(os.path.join(output_dir, prefix+str(key)+'.txt'), 'w', encoding='utf-8') as output_fd:
					for v in value:
						output_fd.write(str(v) + '\n')
					output_fd.close()	
		return

	def word_count(self, value):

		output = []
		for line in value:
			for word in line.split(" "):
				word = word.strip(string.punctuation)
				if word:
					output.append((word, 1))

		return output

	def percent_access_URL_1(self, value):

		output = []
		for line in value:
			output.append((line.split(" ")[10], 1))
		return output

	def percent_access_URL_2(self, value):

		output = []
		for line in value:
			output.append((1, line))
		return output


class ReduceTask(object):

	def __init__(self, inter_dir=None, prefix=None, application=None, dest_filename=None, delete=None):
		"""
		Initiaze the reduce task that handles the reduce phase for mutiplie applications
		"""
	
		d = self.read_keys(inter_dir, prefix, application)
		self.write_key_value_agg(d, dest_filename)
		if delete:
			self.delete_inter_files(inter_dir)
		return

	def read_keys(self, inter_dir=None, prefix=None, application=None):
		"""
		Read in intermediate files.
		"""
	
		kv_red = {}
		if inter_dir and prefix and application:
			inter_files_tup = [f for f in os.walk(inter_dir)]
			inter_files = inter_files_tup[0][2]
			for i, intf in enumerate(inter_files):
#				if i / len(inter_files) >= 0.5:
#					send_progress_to_master('localhost',
#											10000,
#											'Progress Map Task {} {}'.format(i/len(inter_files),
#																			socket.gethostbyname())
#											)
				with open(os.path.join(inter_dir, intf), 'r') as intfd:
					values = [v for v in intfd.readlines()]
					key = intf.split(prefix)[1].strip(".txt")
					if application == 'word_count':
						kv_red[key] = self.word_count(key, [int(v) for v in values])
					elif application == 'purl1':
						kv_red[key] = self.percent_access_URL_1(key, [int(v) for v in values])
					elif application == 'purl2':
						kv_red[key] = self.percent_access_URL_2(key, values)
					else:
						print("Please enter a supported application for the reduce phase")
				intfd.close()
		return kv_red

	def write_key_value_agg(self, kv_red=None, dest_filename=None):
		
		if dest_filename and kv_red:
			with open(dest_filename, 'w') as fd:
				for k, v in kv_red.items():
					fd.write(str(k) + " " + str(v) + "\n")
				fd.close()
		else:
			print("Please enter an appropriate destination filename or kv_red")	
		return 

	def delete_inter_files(self, inter_dir=None):

		if inter_dir:
			filelist = [f for f in os.listdir(inter_dir)]
			for f in filelist:	
				os.remove(os.path.join(inter_dir, f))
		return	

	def word_count(self, key, value):
		return functools.reduce(operator.add, value)

	def percent_access_URL_1(self, key, value):
		return functools.reduce(operator.add, value)		

	def percent_access_URL_2(self, key, value):
		total_accesses = functools.reduce(operator.add, [v[1] for v in value])
		percent_accesses = map(lambda x: (x[0], float(x[1]/total_accesses), value))
		return percent_accesses 


def send_progress_to_master(master_hostname=None, master_port=None, msg=None):
	send_prog_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	send_prog_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
	send_prog_sock.connect((master_hostname, master_port))
	send_prog_sock.sendall(pickle.dumps(msg))
	send_prog_sock.recv(16)
	send_prog_sock.close()
	

if __name__ == '__main__':
	num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory = sys.argv[1:]
	map_task = MapTask(sdfs_src_directory, 'word_count', prefix=sdfs_intermediate_filename_prefix)	
# num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete = sys.argv[1:]	
#	reduce_task = ReduceTask('inter_pairs', sdfs_intermediate_filename_prefix, 'word_count', sdfs_dest_filename, delete)
 

import sys
import os
import collections
import functools
import subprocess
import io
import ast
import socket
import global_variables
import cPickle as pickle
import itertools
import operator
import time

sys.path.insert(0, 'apps/')

from word_count_reduce import word_count
from percent_access_URL_1_reduce import percent_access_URL_1
from percent_access_URL_2_reduce import percent_access_URL_2
from tcp_client import tcp_client


class ReduceTask(object):

	def __init__(self, inter_dir=None, prefix=None, application=None, dest_filename=None, delete=None, rm_addr=None,
				 rm_port=None):
		"""
		Initiaze the reduce task that handles the reduce phase for mutiplie applications
		"""
		self.rm_addr = rm_addr
		self.rm_port = rm_port

		d = self.read_keys(inter_dir, prefix, application)
		# for key, value in d.items():
		# 	print("Reduce output key and value: ", key, value)
		output_name = self.write_key_value_agg(d, dest_filename)
		self.put_file_into_sdfs(output_name)
		if delete:
			self.delete_inter_files(inter_dir)

		return

	def read_keys(self, inter_dir='../sdfs', prefix=None, application=None):
		"""
		Read in intermediate files.
		"""

		kv_red = {}
		if inter_dir and prefix and application:
			# inter_files_tup = [f for f in os.walk('sdfs')]
			# inter_files = inter_files_tup[0][2]
			inter_files = [f for f in os.listdir(inter_dir) if f.find(prefix) != -1]
			# print("[ReduceTask] inter_files: ", inter_files)
			size = len(inter_files)

			if size:
				client = tcp_client()
				client.connect(self.rm_addr, self.rm_port)
				client.send_progress('reduce', 0, size)
				client.sock.close()

			for i, intf in enumerate(inter_files):
				conf = self.check_noDuplicate(intf)
				if conf == 'Y':
					with open(os.path.join(inter_dir, intf), 'r') as intfd:
						values = [v.strip('\n') for v in intfd.readlines()]
						key = intf.split(prefix)[1].strip(".txt")
						# print("key in reduce: ", key, values)
						if application == 'word_count_reduce.py':
							kv_red[key] = word_count(key, [int(v) for v in values])
						elif application == 'percent_access_URL_1_reduce.py':
							kv_red[key] = percent_access_URL_1(key, [int(v) for v in values])
						elif application == 'percent_access_URL_2_reduce.py':
							kv_red[key] = percent_access_URL_2(key, values)
						else:
							print("Please enter a supported application for the reduce phase")
						# print(kv_red[key])
						# kv_red[key] = subprocess.call(['python', 'apps/'+str(application),\
						#  								key, [int(v) for v in values]],\
						# 								stdout=subprocess.PIPE).stdout.decode()
						# kv_red[key] = ast.literal_eval(subprocess.Popen(['python', 'apps/' + application, key] + values, stdout=subprocess.PIPE).stdout.readline())
						# time.sleep(1)
					intfd.close()
				client = tcp_client()
				client.connect(self.rm_addr, self.rm_port)
				client.send_progress('reduce', i + 1, size)
				client.sock.close()

		return kv_red

	def write_key_value_agg(self, kv_red=None, dest_filename=None):

		vm_number = socket.gethostname().split("-")[3].split(".")[0]
		if dest_filename and kv_red:
			with open("../local/" + dest_filename + '_' + vm_number + '.txt', 'w') as fd:
				for k, v in kv_red.items():
					fd.write(str(k) + " " + str(v) + "\n")
				fd.close()
		else:
			print("Please enter an appropriate destination filename or kv_red")

		return dest_filename + '_' + vm_number + '.txt'

	def delete_inter_files(self, inter_dir=None):

		if inter_dir:
			filelist = [f for f in os.listdir(inter_dir)]
			for f in filelist:
				os.remove(os.path.join(inter_dir, f))
		return

	def put_file_into_sdfs(self, output_fn):

		put_client = tcp_client()
		put_client.connect(self.rm_addr, global_variables.port_map["sdfs"][0])
		put_client.req_put_file(output_fn, output_fn)
		put_client.sock.close()

	def check_noDuplicate(self, filename):

		client = tcp_client()
		client.connect(self.rm_addr, self.rm_port)
		pak = {
			"type": "Check",
			"arguments": filename
		}
		client.sock.sendall(pickle.dumps(pak))
		resp = client.sock.recv(4096)
		# print("Resource manager response: ", resp)
		client.sock.close()
		return resp

# def word_count(self, key, value):
# 	return functools.reduce(operator.add, value)
#
# def percent_access_URL_1(self, key, value):
# 	return functools.reduce(operator.add, value)
#
# def percent_access_URL_2(self, key, value):
# 	total_accesses = functools.reduce(operator.add, [v[1] for v in value])
# 	percent_accesses = map(lambda x: (x[0], float(x[1]/total_accesses), value))
# 	return percent_accesses

# if __name__ == '__main__':
#
# 	# num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete = sys.argv[1:]
# 	num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete = 0, 'test_prefix_', '../sdfs/test_dir2_test_output.txt', 0
# 	reduce_task = ReduceTask('../sdfs', sdfs_intermediate_filename_prefix, 'percent_access_URL_1_reduce.py', sdfs_dest_filename, delete)

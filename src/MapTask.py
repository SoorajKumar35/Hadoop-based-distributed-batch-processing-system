import sys
import os
import collections
import functools
import subprocess
import io
import ast
import itertools
import cPickle as pickle
import time

sys.path.insert(0, 'apps/')

from word_count_map import word_count
from percent_access_URL_1_map import percent_access_URL_1
from percent_access_URL_2_map import percent_access_URL_2
from tcp_client import tcp_client


class MapTask(object):
    """
	Class that defines the Map functionality of the MapleJuice System
	"""

    def __init__(self, shard_dir=None, application=None, prefix=None, rm_addr=None, rm_port=None):
        """
		Initialize the Map function that handles the map phase for multiple applications
		"""

        encoding = None
        if application == 'percent_access_URL_1_map.py':
            encoding = 'latin-1'
        else:
            encoding = 'utf-8'

        self.rm_addr = rm_addr
        self.rm_port = rm_port

        if shard_dir:
            output = self.produce_output_tuples(shard_dir, encoding=encoding, application=application)
            # print("shard output: ", output)
            self.write_output(output, prefix=prefix, output_dir='../inter_dir')
        else:
            print("Please provide the directory where the shards are stored.")

    def produce_output_tuples(self, shard_dir=None, encoding=None, application=None):

        shard_output = []
        shard_files_list = [i for i in os.listdir('../sdfs')]
        shard_files = [f for f in shard_files_list if f.find(shard_dir) != -1]

        client = tcp_client()
        client.connect(self.rm_addr, self.rm_port)
        client.send_progress('map', 0, len(shard_files))
        client.sock.close()

        for i, shard in enumerate(shard_files):
            conf = self.check_noDuplicate(shard)
            # print("Currently in MapTask")
            if conf == 'Y':
                with open(os.path.join('../sdfs', shard), 'r') as shard_fd:
                    while True:
                        ten_lines = itertools.islice(shard_fd, 10)
                        value = [line.strip('\n') for line in ten_lines if line != '\n']
                        # print("ten_lines: ", value)
                        if not value:
                            break
                        if application == 'word_count_map.py':
                            shard_output += word_count(value)
                        elif application == 'percent_access_URL_1_map.py':
                            shard_output += percent_access_URL_1(value)
                        elif application == 'percent_access_URL_2_map.py':
                            shard_output += percent_access_URL_2(value)
                        else:
                            print("Please enter a correct application")
                            return
                        time.sleep(1)
                        # application_output = ast.literal_eval(subprocess.Popen(['python', str('apps/') + application] + value,
                        #                                                        stdout=subprocess.PIPE).stdout.readline())
                        # shard_output += application_output
                    shard_fd.close()

            client = tcp_client()
            client.connect(self.rm_addr, self.rm_port)
            client.send_progress('map', i + 1, len(shard_files))
            client.sock.close()


        return shard_output

    def write_output(self, shard_keyval_pairs=None, prefix=None, output_dir='../inter_dir'):

        # if not os.path.exists(output_dir):
        # 	os.makedirs(output_dir)

        key_values_dict = collections.defaultdict(list)
        # if shard_keyval_pairs:
        #     functools.reduce(lambda self, pair: key_values_dict[pair[0]].append(pair[1]), shard_keyval_pairs)
        for key, value in shard_keyval_pairs:
            key_values_dict[key].append(value)

        if prefix and output_dir:
            for key, value in key_values_dict.items():
                # print(key, len(value))
                with open(os.path.join(output_dir, prefix + str(key) + '.txt'), 'w') as output_fd:
                    for v in value:
                        output_fd.write(str(v) + '\n')
                    output_fd.close()
        return

    def check_noDuplicate(self, filename):

        client = tcp_client()
        client.connect(self.rm_addr, self.rm_port)
        pak = {
            "type":"Check",
            "arguments":filename
        }
        client.sock.sendall(pickle.dumps(pak))
        resp = client.sock.recv(4096)
        # print("Resource manager response: ", resp)
        client.sock.close()
        return resp

# def word_count(self, value):
#
# 	output = []
# 	for line in value:
# 		for word in line.split(" "):
# 			word = word.strip(string.punctuation)
# 			if word:
# 				output.append((word, 1))
# 	return output
#
# def percent_access_URL_1(self, value):
#
# 	output = []
# 	for line in value:
# 		output.append((line.split(" ")[10], 1))
# 	return output
#
# def percent_access_URL_2(self, value):
#
# 	output = []
# 	for line in value:
# 		output.append((1, line))
# 	return output


# if __name__ == '__main__':
#     # num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory = sys.argv[1:]
#     num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory, application = 0, 'test_prefix_', 'test_dir_word_count_', 'word_count_map.py'
#     map_task = MapTask(sdfs_src_directory, application, prefix=sdfs_intermediate_filename_prefix)

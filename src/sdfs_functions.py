
import global_variables

import socket
import time
from collections import defaultdict
import cPickle as pickle
# import pickle
import sys
import argparse
import random
import subprocess
import threading
from datetime import datetime
from tcp_client import tcp_client
import math
import os
import hashlib

import failure_detector_functions

# from failure_detector_functions import *
# from failure_detector_functions import parseIP

def write_file_threaded(sdfs_filename, data_from_datanode):

	if os.path.exists("../sdfs/" + sdfs_filename):
		with open("../sdfs/" + sdfs_filename, 'a') as f:
			f.write(data_from_datanode['file'])
	else:
		with open("../sdfs/" + sdfs_filename, 'wb') as f:
			f.write(data_from_datanode['file'])

def replication(deleted_hostname):

	# Begin replication by using the meta-store to get all files that were stored
	# on the failed node

	files_in_delmachine, files_to_ts = [], {}
	for file, file_array_map in global_variables.file_to_machine_map.items():
		for idx, j in enumerate(file_array_map):

			# Check if the hostname is the same as the hostname to be deleted
			# Also, check if we need to make a replica. If there are 4 or more replicas,
			# then it would be resistant to 3 simulataneous failures.
			# Delete any information about machine in filename list
			if j["hostname"] == deleted_hostname and (len(file_array_map) - 1) <= 3:
				files_in_delmachine.append(file)
				del global_variables.file_to_machine_map[file][idx]

	# Get filenames to be replicated and the hostnames where they are located.
	# Also, get hostnames where they are not located.

	fm_pairs, nodes_to_store = [], []
	membership_list_hostnames = [socket.gethostbyaddr(parseIP(m))[0] for m in global_variables.membership_list]
	for file in files_in_delmachine:
		try:
			print("global_variables.file_to_machine_map[file]", global_variables.file_to_machine_map[file])
		except Exception, e:
			pass

		fm_pairs.append((file, global_variables.file_to_machine_map[file][0]["hostname"])) # Double check this statement
		files_to_ts[file] = global_variables.file_to_machine_map[file][0]["timestamp"]
		file_hostnames = [t["hostname"] for t in global_variables.file_to_machine_map[file]]
		nodes_without_replica = list(set(membership_list_hostnames) ^ set(file_hostnames))

		if nodes_without_replica:
			nodes_to_store.append(nodes_without_replica[0])

	# Then, we send replicate commands
	# with a node that contains the filename and the hostname that contains the file replica
	print("files in del machine", files_in_delmachine, "\n")
	print("nodes to store", nodes_to_store, "\n")
	print("fm_pairs", fm_pairs, "\n")

	if nodes_to_store:
		for i, pair in enumerate(fm_pairs):

			replicate_string = pickle.dumps({
				"type": "replicate",
				"filename": pair[0],
				"hostname": pair[1]
			})

			client = tcp_client()
			client.connect(nodes_to_store[i], global_variables.port_map['sdfs'][0])
			client.send(replicate_string)
			client.sock.close()

			# Change meta info accordingly
			global_variables.file_to_machine_map[pair[0]].append({
				"timestamp": files_to_ts[pair[0]],
				"hostname": nodes_to_store[i]
			})

	else:
		print("There are no nodes that currently don't have a replica of the files\n thus, we wont't replicate those files.")
		pass

	for file in files_in_delmachine:
		for node in global_variables.membership_list:
			node_hostname = socket.gethostbyaddr(parseIP(node))[0]
			# print("node hostname: ", node_hostname)
			add_meta_pak_string = pickle.dumps({
				"type": "add_meta",
				"filename": file,
				"file_to_machine_map_array": global_variables.file_to_machine_map
			})
			client = tcp_client()
			client.connect(node_hostname, global_variables.port_map['sdfs'][0])
			client.send(add_meta_pak_string)
			client.sock.close()


def sdfs_partitioner(local_filename):
	"""
	Function that picks which nodes in the membership list to send the local file
	that we want to store in SDFS.

	We will pick at least 4 nodes if possible given the number of nodes currently in the membership
	list. This is to ensure resilliency in the case of upto 3 simultaneous failures.
	"""

	# Computes the hexadecimal SHA-256 hash, finds the binary rep, converts to int, and mods with the length of membership list
	sha_hash_fn = hashlib.sha256()
	sha_hash_fn.update(local_filename)
	target_node = int(sha_hash_fn.hexdigest()[:10], 16) % len(global_variables.membership_list)
	return target_node


def quorum(local_filename, is_write):
	"""
	Function that returns the set of nodes to either write or read from. In our implementation, we
	will be using a write majority quorum. Thus,

	#W = #R + 1 and W+R = N + 1
	"""

	target = sdfs_partitioner(local_filename)

	# print "Target: ", target

	N, Np1 = len(global_variables.membership_list), len(global_variables.membership_list) + 1

	# Find W and R
	R, W = math.ceil(float(Np1 + 1) / 2), Np1 - math.ceil(float(Np1 + 1) / 2)

	#
	# print "W: ", W, "R: ", R
	# print "target: ", target
	# print int(-W / 2), int(-R / 2)

	# Find quorum indices for either write or read
	if not is_write:
		quorum_idx = [(target + i) % N for i in range(int(math.ceil(-W / 2)), int(W / 2) + 1)] \
			if N % 2 != 0 else \
			[(target + i) % N for i in range(int(math.ceil(-W / 2)), int(W / 2) + 1)]

	else:
		quorum_idx = [(target + i) % N for i in range(int(math.ceil(-R / 2)) + 1, int(R / 2) + 1)] \
			if N % 2 != 0 else \
			[(target + i) % N for i in range(int(math.ceil(-R / 2)), int(R / 2) + 1)]

		# if len(quorum_idx) < 4:
		# 	nodes_not_in_quorum = list(set(quorum_idx) ^ set([i for i in range(len(global_variables.membership_list))]))
		# 	quorum_idx += nodes_not_in_quorum[:4-len(quorum_idx)]

	# print "Current indices: ", quorum_idx

	# if is_write:
	# 	if W < 4 and len(membership_list) >= 4:
	# 		W = 4
	# 	quorum_idx = random.sample(range(len(membership_list)), k=int(W))
	# else:
	# 	quorum_idx = random.sample(range(len(membership_list)), k=int(R))

	quorum_idx = quorum_idx[:2]
	# Return set to either write to or read from
	# print("[Quorum]: Length of qourum; ", len(set([socket.gethostbyaddr(parseIP(global_variables.membership_list[i]))[0] for i in quorum_idx])))
	return set([socket.gethostbyaddr(parseIP(global_variables.membership_list[i]))[0] for i in quorum_idx])

def parseIP(id):
	return id.split(":")[0]

def tcp_cmd_recv_threaded(data_node_sock, data_node_addr, port=8888):

	# print("data_node_addr: ", socket.gethostbyaddr(data_node_addr[0])[0])
	data_node_tcp_client = tcp_client(data_node_sock)
	data_from_datanode = data_node_tcp_client.recv()

	# Unpickle the incoming client packet and extract the command along with
	# the sdfs filename
	data_from_datanode = pickle.loads(data_from_datanode)
	command = data_from_datanode['type']
	sdfs_filename = data_from_datanode['filename']

	# If the current node is the active name node
	if (failure_detector_functions.get_id() == global_variables.membership_list[0]):

		if command == "get":
			replica_info = global_variables.file_to_machine_map[sdfs_filename]
			# Send back dictionary corresponding to filename
			data_node_tcp_client.send(pickle.dumps(replica_info))

		elif command == "put":
			# print("[TCP cmd recv]: Got put cmd")
			update_timestamp = time.time()

			# Check if any replicas of the filename exist already in SDFS
			if global_variables.file_to_machine_map[sdfs_filename]:

				# We are updating the current replicas of the file and writing the
				# file anew to new nodes included in the write quorum

				# Get maximum timestamp and machines that contain the file already
				latest_timestamp = max([t["timestamp"] for t in global_variables.file_to_machine_map[sdfs_filename]])
				current_hostnames = [t["hostname"] for t in global_variables.file_to_machine_map[sdfs_filename]]

				# Check if the most recent update to the file was within 60 seconds
				if time.time() - latest_timestamp < 60:

					# Send wait message to client to request confirmation for file update
					wait_string = pickle.dumps({
						"type": "wait"
					})
					data_node_tcp_client.send(wait_string)
					wait_resp = data_node_tcp_client.recv()
					if wait_resp != "yes" and wait_resp != "Yes":
						# The user would not like to go through with the update,
						# thus, we close the conversation gracefully and accept
						# a new connection
						data_node_tcp_client.sock.close()
						return

				# Update the meta information
				write_quorum = quorum(sdfs_filename, True)
				for node in write_quorum:
					if node not in current_hostnames:
						global_variables.file_to_machine_map[sdfs_filename].append({"hostname": node, "timestamp": update_timestamp})
					else:
						for t in global_variables.file_to_machine_map[sdfs_filename]:
							if t["hostname"] == node:
								t["timestamp"] = update_timestamp
								break

				# Send the acknowledgement to the client so that the client can begin downloading.
				write_quorum_string = pickle.dumps({
					"type": "put_ack",
					"filename": sdfs_filename,
					"hostnames": write_quorum
				})
				data_node_tcp_client.send(write_quorum_string)

			else:

				# This means that the file is a brand new file in SDFS. Thus, we simply insert
				# to the meta information and send an acknowledgement
				write_quorum = quorum(sdfs_filename, True)
				for node in write_quorum:
					global_variables.file_to_machine_map[sdfs_filename].append({"hostname": node, "timestamp": update_timestamp})

				write_quorum_string = pickle.dumps({
					"type": "put_ack",
					"filename": sdfs_filename,
					"hostnames": write_quorum
				})
				# print("[TCP cmd recv]: Dumped put_ack")
				data_node_tcp_client.send(write_quorum_string)
				# time.sleep(1)

			# Now that we have either updated or inserted a new file into the meta information
			# and we have sent a put_ack command back to the client, we send add_meta command
			# to all our backup meta stores so that they are all consistent

			# Create the add_meta packet and encode it
			add_meta_pak_string = pickle.dumps({
				"type": "add_meta",
				"filename": sdfs_filename,
				"file_to_machine_map_array": global_variables.file_to_machine_map
			})

			# Create a tcp client connection with every node except for the current node and
			# send them an add meta information command with the necc. data

			for node in global_variables.membership_list:
				if socket.gethostbyaddr(parseIP(node))[0] == socket.gethostname():
					continue
				add_meta_tcp_client = tcp_client()
				add_meta_tcp_client.connect(socket.gethostbyaddr(parseIP(node))[0], global_variables.port_map['sdfs'])
				add_meta_tcp_client.send(add_meta_pak_string)
				add_meta_tcp_client.sock.close()


		elif command == "delete":

			# Send delete data command to every machine that is included in the
			# filename to hostname map.

			file_to_machine_map_hostnames = [t["hostname"] for t in global_variables.file_to_machine_map[sdfs_filename]]
			for node in file_to_machine_map_hostnames:

				# If hostname is of the current active namenode, then we simply delete the file locally
				# Assume that the split -> sooraj@illinois.edu -> sooraj, illinois.edu
				if node == socket.gethostname().split(".")[0]:
					subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])
					return

				# Packet of the delete command -> command + filename
				delete_data_string = pickle.dumps({
					"type": "delete_data",
					"filename": sdfs_filename
				})

				# Create new tcp client connection with machine that contains the file that
				# we want to delete

				delete_cmd_tcp_client = tcp_client()
				delete_cmd_tcp_client.connect(node, global_variables.port_map['sdfs'][0])
				delete_cmd_tcp_client.send(delete_data_string)
				delete_cmd_tcp_client.sock.close()

			# Since our design allows for N meta stores, we send a delete_meta_info command along
			# with a delete_data command in order to ensure that the meta information reflects
			# the changes made with delete

			# The packet for deleting meta information about a particular
			delete_meta_info_string = pickle.dumps({
				"type": "delete_meta",
				"filename": sdfs_filename
			})

			for node in global_variables.membership_list:

				if socket.gethostbyaddr(parseIP(node))[0] == socket.gethostname():
					continue

				# Create new tcp client connect with machine in order to delete meta
				# information on that client about the current sdfs filename

				delete_meta_info_client = tcp_client()
				delete_meta_info_client.connect(socket.gethostbyaddr(parseIP(node))[0], global_variables.port_map['sdfs'])
				delete_meta_info_client.send(delete_meta_info_string)
				delete_meta_info_client.sock.close()

			# Finally, we remove the filename from our meta-information
			# That is, we remove it from filename to hostname mapping for the active namenode
			del global_variables.file_to_machine_map[sdfs_filename]

		elif command == "ls":

			# Get hostnames where current replicas of sdfs file is being stored
			hostnames_from_sdfsfn = [t["hostname"] for t in global_variables.file_to_machine_map[sdfs_filename]]

			# Pickle the filenames to convert to string for TCP transfer
			hostnames_from_sdfsfn_str = pickle.dumps(hostnames_from_sdfsfn)

			# Respond to client with the hostnames
			data_node_tcp_client.send(hostnames_from_sdfsfn_str)

		if command == "download":
			print("got download")
			# We reply to the requesting data node by transferring the sdfs file
			# from our sdfs folder via TCP transfer. This is encapsulated in the
			# tcp_client class itself.
			# This is in the get command execution
			data_node_tcp_client.resp_download_file(sdfs_filename)

		elif command == "put_file":
			# print("[SDFS functions]: Got put_file")
			# A client is attempting to write a local file into SDFS. Thus,
			# we receive the data and then write it into the local SDFS
			# directory. The node will serve as a replica of the current sdfs file
			# todo threading
			write_file_threaded(sdfs_filename, data_from_datanode)

		elif command == "delete_data":

			# Delete the sdfs file from the local sdfs file store
			subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])

		elif command == "replicate":

			# This means that a node has either left the network or
			# has crashed. Thus, we will utilize a download command
			# to replicate the file on this node.

			# Create new connection with replica node and download the data into the SDFS folder
			print("The sdfs filename after replicate: ", sdfs_filename)
			replicate_tcp_client = tcp_client()
			replicate_tcp_client.connect(data_from_datanode["hostname"], global_variables.port_map['sdfs'][0])
			replicate_tcp_client.req_download_file(sdfs_filename, sdfs_filename, location='../sdfs')
			replicate_tcp_client.sock.close()
	else:

		if command == "download":
			print("got download")
			# We reply to the requesting data node by transferring the sdfs file
			# from our sdfs folder via TCP transfer. This is encapsulated in the
			# tcp_client class itself.
			# This is in the get command execution
			data_node_tcp_client.resp_download_file(sdfs_filename)

		elif command == "put_file":
			# print("got put_file")
			# A client is attempting to write a local file into SDFS. Thus,
			# we receive the data and then write it into the local SDFS
			# directory. The node will serve as a replica of the current sdfs file
			threading.Thread(target=write_file_threaded, args=(sdfs_filename, data_from_datanode)).start()

		elif command == "delete_data":

			# Delete the sdfs file from the local sdfs file store
			subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])

		elif command == "add_meta":

			# This command means that the active namenode has received a put command to either update or
			# insert a new file. Thus, we update the backup metastore here to reflect the recent
			# changes made on the active namenode
			print("Got add meta packet")
			new_meta_data = data_from_datanode['file_to_machine_map_array']
			global_variables.file_to_machine_map = new_meta_data

		elif command == "delete_meta":

			# This command means that the active namenode has received a delete command for a
			# particular sdfs file. Thus, the file meta information should be deleted in all
			# replica meta stores, including this one.
			del global_variables.file_to_machine_map[sdfs_filename]

		elif command == "replicate":

			# This means that a node has either left the network or
			# has crashed. Thus, we will utilize a download command
			# to replicate the file on this node.

			# Create new connection with replica node and download the data into the SDFS folder
			print("Hostname to replicate from: ", data_from_datanode["hostname"])
			print("sdfs filename to replicate: ", sdfs_filename)
			replicate_tcp_client = tcp_client()
			replicate_tcp_client.connect(data_from_datanode["hostname"], global_variables.port_map['sdfs'][0])
			replicate_tcp_client.req_download_file(sdfs_filename,
													sdfs_filename,
													location='../sdfs')
			replicate_tcp_client.sock.close()

	# At the end of the conversation, the active namenode gracefully closes
	# the socket and ends the conversation
	data_node_tcp_client.sock.close()


def tcp_command_recieve(port=8888):
	"""
	This function is responsible for receiving commands for the SDFS. The commands are dependent on whether
	the current nodes is an introducer node or is another datanode.
	"""

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
	print("[TCP cmd recv]: hostname {} and port {}".format(socket.gethostname(), port))
	sock.bind((socket.gethostname(), port))
	sock.listen(1)

	while True:

		# Accept a connection from a client to begin conversation
		# from a data node client

		data_node_sock, data_node_addr = sock.accept()
		# print("data_node_addr: ", socket.gethostbyaddr(data_node_addr[0])[0])
		data_node_tcp_client = tcp_client(data_node_sock)
		data_from_datanode = data_node_tcp_client.recv()

		# Unpickle the incoming client packet and extract the command along with
		# the sdfs filename
		data_from_datanode = pickle.loads(data_from_datanode)
		command = data_from_datanode['type']
		sdfs_filename = data_from_datanode['filename']

		# If the current node is the active name node
		if (failure_detector_functions.get_id() == global_variables.membership_list[0]):

			if command == "get":
				replica_info = global_variables.file_to_machine_map[sdfs_filename]
				# Send back dictionary corresponding to filename
				data_node_tcp_client.send(pickle.dumps(replica_info))

			elif command == "put":
				# print("[TCP cmd recv]: Got put cmd")
				update_timestamp = time.time()

				# Check if any replicas of the filename exist already in SDFS
				if global_variables.file_to_machine_map[sdfs_filename]:

					# We are updating the current replicas of the file and writing the
					# file anew to new nodes included in the write quorum

					# Get maximum timestamp and machines that contain the file already
					latest_timestamp = max(
						[t["timestamp"] for t in global_variables.file_to_machine_map[sdfs_filename]])
					current_hostnames = [t["hostname"] for t in global_variables.file_to_machine_map[sdfs_filename]]

					# Check if the most recent update to the file was within 60 seconds
					if time.time() - latest_timestamp < 60:

						# Send wait message to client to request confirmation for file update
						wait_string = pickle.dumps({
							"type": "wait"
						})
						data_node_tcp_client.send(wait_string)
						wait_resp = data_node_tcp_client.recv()
						if wait_resp != "yes" and wait_resp != "Yes":
							# The user would not like to go through with the update,
							# thus, we close the conversation gracefully and accept
							# a new connection
							data_node_tcp_client.sock.close()
							return

					# Update the meta information
					write_quorum = quorum(sdfs_filename, True)
					for node in write_quorum:
						if node not in current_hostnames:
							global_variables.file_to_machine_map[sdfs_filename].append(
								{"hostname": node, "timestamp": update_timestamp})
						else:
							for t in global_variables.file_to_machine_map[sdfs_filename]:
								if t["hostname"] == node:
									t["timestamp"] = update_timestamp
									break

					# Send the acknowledgement to the client so that the client can begin downloading.
					write_quorum_string = pickle.dumps({
						"type": "put_ack",
						"filename": sdfs_filename,
						"hostnames": write_quorum
					})
					data_node_tcp_client.send(write_quorum_string)

				else:

					# This means that the file is a brand new file in SDFS. Thus, we simply insert
					# to the meta information and send an acknowledgement
					write_quorum = quorum(sdfs_filename, True)
					for node in write_quorum:
						global_variables.file_to_machine_map[sdfs_filename].append(
							{"hostname": node, "timestamp": update_timestamp})

					write_quorum_string = pickle.dumps({
						"type": "put_ack",
						"filename": sdfs_filename,
						"hostnames": write_quorum
					})
					# print("[TCP cmd recv]: Dumped put_ack")
					data_node_tcp_client.send(write_quorum_string)
			# time.sleep(1)

			# Now that we have either updated or inserted a new file into the meta information
			# and we have sent a put_ack command back to the client, we send add_meta command
			# to all our backup meta stores so that they are all consistent

				# Create the add_meta packet and encode it
				add_meta_pak_string = pickle.dumps({
					"type": "add_meta",
					"filename": sdfs_filename,
					"file_to_machine_map_array": global_variables.file_to_machine_map
				})

				# Create a tcp client connection with every node except for the current node and
				# send them an add meta information command with the necc. data

				for node in global_variables.membership_list:
					if socket.gethostbyaddr(parseIP(node))[0] == socket.gethostname():
						continue
					add_meta_tcp_client = tcp_client()
					add_meta_tcp_client.connect(socket.gethostbyaddr(parseIP(node))[0], global_variables.port_map['sdfs'][0])
					add_meta_tcp_client.send(add_meta_pak_string)
					add_meta_tcp_client.sock.close()

			elif command == "delete":

				# Send delete data command to every machine that is included in the
				# filename to hostname map.

				file_to_machine_map_hostnames = [t["hostname"] for t in
												 global_variables.file_to_machine_map[sdfs_filename]]
				for node in file_to_machine_map_hostnames:

					# If hostname is of the current active namenode, then we simply delete the file locally
					# Assume that the split -> sooraj@illinois.edu -> sooraj, illinois.edu
					if node == socket.gethostname().split(".")[0]:
						subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])
						return

					# Packet of the delete command -> command + filename
					delete_data_string = pickle.dumps({
						"type": "delete_data",
						"filename": sdfs_filename
					})

					# Create new tcp client connection with machine that contains the file that
					# we want to delete

					delete_cmd_tcp_client = tcp_client()
					delete_cmd_tcp_client.connect(node, global_variables.port_map['sdfs'][0])
					delete_cmd_tcp_client.send(delete_data_string)
					delete_cmd_tcp_client.sock.close()

				# Since our design allows for N meta stores, we send a delete_meta_info command along
				# with a delete_data command in order to ensure that the meta information reflects
				# the changes made with delete

				# The packet for deleting meta information about a particular
				delete_meta_info_string = pickle.dumps({
					"type": "delete_meta",
					"filename": sdfs_filename
				})

				for node in global_variables.membership_list:

					if socket.gethostbyaddr(parseIP(node))[0] == socket.gethostname():
						continue

					# Create new tcp client connect with machine in order to delete meta
					# information on that client about the current sdfs filename

					delete_meta_info_client = tcp_client()
					delete_meta_info_client.connect(socket.gethostbyaddr(parseIP(node))[0], global_variables.port_map['sdfs'][0])
					delete_meta_info_client.send(delete_meta_info_string)
					delete_meta_info_client.sock.close()

				# Finally, we remove the filename from our meta-information
				# That is, we remove it from filename to hostname mapping for the active namenode
				del global_variables.file_to_machine_map[sdfs_filename]

			elif command == "ls":

				# Get hostnames where current replicas of sdfs file is being stored
				hostnames_from_sdfsfn = [t["hostname"] for t in global_variables.file_to_machine_map[sdfs_filename]]

				# Pickle the filenames to convert to string for TCP transfer
				hostnames_from_sdfsfn_str = pickle.dumps(hostnames_from_sdfsfn)

				# Respond to client with the hostnames
				data_node_tcp_client.send(hostnames_from_sdfsfn_str)

			if command == "download":
				print("got download")
				# We reply to the requesting data node by transferring the sdfs file
				# from our sdfs folder via TCP transfer. This is encapsulated in the
				# tcp_client class itself.
				# This is in the get command execution
				data_node_tcp_client.resp_download_file(sdfs_filename)

			elif command == "put_file":
				# print("[SDFS functions]: Got put_file")
				# A client is attempting to write a local file into SDFS. Thus,
				# we receive the data and then write it into the local SDFS
				# directory. The node will serve as a replica of the current sdfs file
				# todo threading
				threading.Thread(target=write_file_threaded, args=(sdfs_filename, data_from_datanode)).start()

			elif command == "delete_data":

				# Delete the sdfs file from the local sdfs file store
				subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])

			elif command == "replicate":

				# This means that a node has either left the network or
				# has crashed. Thus, we will utilize a download command
				# to replicate the file on this node.

				# Create new connection with replica node and download the data into the SDFS folder
				print("The sdfs filename after replicate: ", sdfs_filename)
				replicate_tcp_client = tcp_client()
				replicate_tcp_client.connect(data_from_datanode["hostname"], global_variables.port_map['sdfs'][0])
				replicate_tcp_client.req_download_file(sdfs_filename, sdfs_filename, location='../sdfs')
				replicate_tcp_client.sock.close()
		else:

			if command == "download":
				print("got download")
				# We reply to the requesting data node by transferring the sdfs file
				# from our sdfs folder via TCP transfer. This is encapsulated in the
				# tcp_client class itself.
				# This is in the get command execution
				data_node_tcp_client.resp_download_file(sdfs_filename)

			elif command == "put_file":
				# print("Got put_file")
				# A client is attempting to write a local file into SDFS. Thus,
				# we receive the data and then write it into the local SDFS
				# directory. The node will serve as a replica of the current sdfs file
				write_file_threaded(sdfs_filename, data_from_datanode)

			elif command == "delete_data":

				# Delete the sdfs file from the local sdfs file store
				subprocess.Popen(['rm', os.path.join('../sdfs', sdfs_filename)])

			elif command == "add_meta":

				# This command means that the active namenode has received a put command to either update or
				# insert a new file. Thus, we update the backup metastore here to reflect the recent
				# changes made on the active namenode
				# print("Got add meta packet")
				new_meta_data = data_from_datanode['file_to_machine_map_array']
				global_variables.file_to_machine_map = new_meta_data

			elif command == "delete_meta":

				# This command means that the active namenode has received a delete command for a
				# particular sdfs file. Thus, the file meta information should be deleted in all
				# replica meta stores, including this one.
				del global_variables.file_to_machine_map[sdfs_filename]

			elif command == "replicate":

				# This means that a node has either left the network or
				# has crashed. Thus, we will utilize a download command
				# to replicate the file on this node.

				# Create new connection with replica node and download the data into the SDFS folder
				print("Hostname to replicate from: ", data_from_datanode["hostname"])
				print("sdfs filename to replicate: ", sdfs_filename)
				replicate_tcp_client = tcp_client()
				replicate_tcp_client.connect(data_from_datanode["hostname"], global_variables.port_map['sdfs'][0])
				replicate_tcp_client.req_download_file(sdfs_filename,
													   sdfs_filename,
													   location='../sdfs')
				replicate_tcp_client.sock.close()

		# At the end of the conversation, the active namenode gracefully closes
		# the socket and ends the conversation
		data_node_tcp_client.sock.close()

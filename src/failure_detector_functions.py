import global_variables

import socket
import time
from collections import defaultdict
# import cPickle as pickle
import pickle
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

from sdfs_functions import *

mem_list_lock = threading.Lock()
heartbeat_lock = threading.Lock()
global_variables.bandwidth_usage = 0


def dissimenate(command):

	# Function that dissimenates either a join/leave/failure
	for node in global_variables.membership_list:
		if node != get_id():
			command_list = node.split(":")
			UDP_IP, UDP_port = command_list[0], int(command_list[1])
			send_packet(command, UDP_IP, UDP_port)


def print_membership_list():
	print("Membership List:")
	for member in global_variables.membership_list:
		print(socket.gethostbyaddr(parseIP(member))[0])


def init_member_list():
	with mem_list_lock:
		global_variables.membership_list = []
		global_variables.membership_list.append(get_id())

def heartbeat_failure_detector():

	while not global_variables.exiting:
		with heartbeat_lock:
			global_variables.heartbeat = dict.fromkeys(global_variables.heartbeat.iterkeys(), False)
			# print(global_variables.heartbeat)
			# set all heartbeats to false
		old_hb = who_heartbeat()
		# who we are responsible for checking

		# wait 4 seconds to recieve hb
		time.sleep(4)

		# check if they pinged
		for id in old_hb:
			if id in global_variables.heartbeat and global_variables.heartbeat[id] == False:
				# make sure we have recieved a hb before trying to delete
				time.sleep(4)
				if id in global_variables.heartbeat and global_variables.heartbeat[id] == False:
					try:
						with heartbeat_lock:
							global_variables.heartbeat.pop(id)
							# dont try to recieve it again
					except KeyError:
						pass

					with mem_list_lock:
						global_variables.membership_list.remove(id)
						 # remove from mem list

					msg = "delete:" + id
					dissimenate(msg)

					if get_id() == global_variables.membership_list[0]:
						replication(socket.gethostbyaddr(parseIP(id))[0])

def get_id():
	# Get unique ID of current node
	IP_addr = socket.gethostbyname(socket.gethostname())
	ID = IP_addr + ":" + str(global_variables.port) + ":" + str(global_variables.timeOfEntry)
	return ID


def send_packet(data, dest_IP, dest_port):
	data_string = pickle.dumps(data)
	global_variables.bandwidth_usage += len(data_string.encode('utf-8'))
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.sendto(data_string, (dest_IP, dest_port))


def contact_introducer(introducer_hostname):
	"""
	This function is used on the process that contacts the introudcer.
	"""

	# We contact the introducer asking to join the network.
	send_command = "add:" + str(get_id())

	# Assuming that the send_packet command gets the IP and addr from the send command itself.
	print(socket.gethostbyname(introducer_hostname))
	send_packet(send_command, socket.gethostbyname(introducer_hostname), global_variables.port)

	# The node now waits for the membership list to be send from the introducer. Just waitin'.


def listen_for_hearbeat_and_disMsgs(sock):

	# Function that listens for heartbeats and dissimenation messages (whether it be joins, leaves or failures)

	sock.bind(("", global_variables.port))

	while not global_variables.exiting:
		data, addr = sock.recvfrom(4096)
		obj = pickle.loads(data)

		if type(obj) is str:
			if (obj == "exit"): return

			# we received a message. Should be in the form <message type>:<id of sender>
			from_id = obj.split(":", 1)[1]

			if (obj.find("heartbeat") != -1):
				# Heart beat case - when token indicates heartbeat
				# Line to extract heartbeat message from data - Assume identifiter in hb_ID
				with heartbeat_lock:
					# print("Object that we are splitting: ", obj)
					global_variables.heartbeat[from_id] = True
				diff = datetime.now() - global_variables.start_time
				diff_sec = diff.days * 24 * 60 * 60 + diff.seconds
				if (diff_sec == 0): diff_sec = 1

			# print(str(bandwidth_usage / diff_sec), " bytes per second")
			elif (obj.find("add") != -1):

				with mem_list_lock:
					if from_id not in global_variables.membership_list:
						global_variables.membership_list.append(from_id)

				# print("The value of global_variables.master: ", global_variables.master)
				if (global_variables.master):
					send_packet(global_variables.membership_list, parseIP(from_id), global_variables.port)
					dissimenate(obj)
					global_variables.resource_manager.nodes_available[socket.gethostbyaddr(addr[0])[0]] = True

			elif (obj.find("delete") != -1):

				try:
					with mem_list_lock:
						global_variables.membership_list.remove(from_id)
					with heartbeat_lock:
						global_variables.heartbeat.pop(from_id)	# act like we got a heartbeat because node has left

					# Put in code for replication due to leave

					# If the current node is the active namenode, then we initiate
					# replication of the node being deleted
					if get_id() == global_variables.membership_list[0]:
						replication(socket.gethostbyaddr(parseIP(from_id))[0])

					if (global_variables.master):
						global_variables.resource_manager.handle_failure(socket.gethostbyaddr(parseIP(from_id))[0])

				except Exception, e:
					# ignore error if id isnt in list
					print("Val doesnt exist", Exception, e)

		else:
			# we recieved a member_list, so update it iif we havent update it before
			with mem_list_lock:
				global_variables.membership_list = obj
				print_membership_list()


def parseIP(id):
	return id.split(":")[0]


def who_heartbeat():

	with mem_list_lock:
		index = global_variables.membership_list.index(get_id())
		N = len(global_variables.membership_list)
		return set([\
			global_variables.membership_list[(index - 1) % N],\
			global_variables.membership_list[(index - 2) % N],\
			global_variables.membership_list[(index + 1) % N],\
			global_variables.membership_list[(index + 2) % N]\
		])
	# we use a set so its unique


def send_heartbeats():

	# Function that sends heartbeats out every 3 seconds
	while not global_variables.exiting:

		msg = "heartbeat:" + get_id()
		dissimenate(msg)
		time.sleep(1)  # making this small will reduce chance of race condition

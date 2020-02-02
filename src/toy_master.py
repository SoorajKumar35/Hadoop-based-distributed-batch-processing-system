import socket
import pickle
import sys

class toy_master(object):

	def __init__(self, task=None):
		send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)	
		send_sock.connect(('localhost', 10000))
		if task=='map':
			send_sock.sendall(pickle.dumps(('map','book','word_count', 'test_prefix')))
		elif task=='reduce':
			send_sock.sendall(pickle.dumps(('reduce', 'inter_pairs', 'test_prefix', 'word_count', 'test_output.txt', 0)) )
		send_sock.recv(16) # Receive j.i.c. nothing goes wrong here
		send_sock.close()


if __name__ == '__main__':	
	tm = toy_master(sys.argv[1])



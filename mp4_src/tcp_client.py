import socket
import os
import pickle
# import thread
import threading
import global_variables
import sys
# from sendfile import sendfile

def parseIP(id):
    return id.split(":")[0]


def raw_input_with_timeout(prompt, timeout=30.0):
    print (prompt),
    timer = threading.Timer(timeout, thread.interrupt_main)
    astring = None
    try:
        timer.start()
        astring = raw_input(prompt)
    except KeyboardInterrupt:
        pass
    timer.cancel()
    return astring

def put_file_threadme(hostname, port, localfilename, sdfsfilename):

    data_client = tcp_client()
    data_client.connect(hostname, port)
    data_client.put_file(localfilename, sdfsfilename)
    data_client.sock.close()

class tcp_client:

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        else:
            self.sock = sock
        self.host = None,
        self.port = None

    def connect(self, host, port):
        self.sock.connect((host, port))
        self.host = host
        self.port = port

    def send(self, msg):
        # send size first
        msg = str(len(msg)) + "::::::" + msg
        return self.sock.sendall(msg)

    def recv(self):
        chonks = []
        data = self.sock.recv(1024)  # should be 24 for an int
        if data == '':
            # print('[Error] Failed to recv on socket')
            return
        size_data = data.split("::::::")
        size = int(size_data[0])
        total_recvd = len(size_data[1])
        # print("size",size)
        # print("total_recvd",total_recvd)
        # print(size_data[1])
        chonks.append(size_data[1])
        while total_recvd < size:
            chonk = self.sock.recv(min(int(size) - total_recvd, 2048))
            if chonk == '':
                # print('[Error] Failed to recv on socket')
                pass
            chonks.append(chonk)
            total_recvd += len(chonk)
        return ''.join(chonks)

    def req_download_file(self, sdfsfilename, localfilename, location='../local'):

        # request download
        pak = {
            "type": "download",
            "filename": sdfsfilename
        }
        self.send(pickle.dumps(pak))

        # wait for data
        data = self.recv()

        # write to local
        with open(os.path.join(location, localfilename), 'wb') as f:
            f.write(data)

    def resp_download_file(self, sdfsfilename):

        # Assume we have already got the request download packet, and now need to send data.
        with open(os.path.join('../sdfs', sdfsfilename), 'rb') as f:
            self.send(f.read())

    def put_file(self, localfilename, sdfsfilename):
        with open('../local/' + localfilename, 'rb') as f:
            # file_size = os.path.getsize('../local/' + localfilename)
            pak = {
                "type": "put_file",
                "filename": sdfsfilename,
                "file": f.read()
            }
            # sendfile(self.sock.fileno(), f.fileno(), 0, file_size)
            self.send(pickle.dumps(pak))
            # print('Sent put_file {} -> {}'.format(localfilename, sdfsfilename))

    def put_file_to_hosts(self, localfilename, sdfsfilename, hostnames, port):

        for i, hostname in enumerate(hostnames):
            if i > 0:
                put_file_threadme(hostname, port, localfilename, sdfsfilename)
            else:
                put_file_threadme(hostname, port, localfilename, sdfsfilename)

    def resp_put_file_ack(self, sdfsfilename):
        pak = {
            "type": "put_ack",
            "hostnames": ['localhost']
        }
        self.send(pickle.dumps(pak))

    def resp_put_file_wait(self, sdfsfilename):
        pak = {
            "type": "wait"
        }
        self.send(pickle.dumps(pak))

    def req_ls(self, sdfsfilename):
        pak = {
            "type": "ls",
            "filename": sdfsfilename
        }
        self.send(pickle.dumps(pak))

    def req_delete(self, sdfsfilename):
        pak = {
            "type": "delete",
            "filename": sdfsfilename
        }
        self.send(pickle.dumps(pak))

    def req_put_file(self, localfilename, sdfsfilename):

        # Put request to name server
        pak = {
            "type": "put",
            "filename": sdfsfilename
        }
        # print("Packet we are sending namenode: ", pak)
        self.send(pickle.dumps(pak))

        # Name node responds with ack (should put) or wait (user must confirm)
        data = pickle.loads(self.recv())

        if data['type'] == 'put_ack':

            # Terminate tcp connection because
            # print('[TCP client req_put_file]: Put_ack recieved')
            hostnames = []
            for id in data['hostnames']:
                hostnames.append(parseIP(id))

            self.put_file_to_hosts(localfilename, sdfsfilename, hostnames, global_variables.port_map['sdfs'][0])

        elif data['type'] == 'wait':
            # should_overwrite = raw_input_with_timeout("Input y if you want to overwrite file.")
            should_overwrite = 'y'
            if should_overwrite == 'y':
                self.send("yes")
                data = pickle.loads(self.recv())
                if data['type'] == 'put_ack':
                    self.put_file_to_hosts(localfilename, sdfsfilename, data['hostnames'],
                                           global_variables.port_map['sdfs'][0])
            else:
                self.send("no")

    def get_file(self, sdfsfilename, localfilename):
        pak = {
            "type": "get",
            "filename": sdfsfilename
        }
        self.send(pickle.dumps(pak))
        hostnames = pickle.loads(self.recv())
        if len(hostnames) != 0:
            max_value = 0
            max_index = 0
            for i, host_o in enumerate(hostnames):
                if max_value < host_o['timestamp']:
                    max_value = host_o['timestamp']
                    max_index = i
            dl_client = tcp_client()
            dl_client.connect(hostnames[max_index]['hostname'], global_variables.port_map['sdfs'][0])
            dl_client.req_download_file(sdfsfilename, localfilename)
        else:
            print("[Error] No file found")

    # MP4 fucntions

    def send_maple_command(self, maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory):
        pak = {
            "type": "maple",
            "arguments": (maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory)
        }
        # print("Message tuple: ", msg_tup)
        # print("[maple] Size in bytes: ", sys.getsizeof(pak))
        self.sock.sendall(pickle.dumps(pak))

    def send_juice_command(self, juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename,
                           delete_input):
        pak = {
            "type": "juice",
            "arguments": (juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input)
        }
        # print("[juice] Size in bytes: ", sys.getsizeof(pak))
        self.sock.sendall(pickle.dumps(pak))

    def send_progress(self, type, current_file, total_files):
        pak = {
            "type": "progress",
            "arguments": ("progress", type, current_file, total_files)
        }
        self.sock.sendall(pickle.dumps(pak))

    # def send_done(self):
    #     pak = {
    #         "type": "progress",
    #         "arguments": ("task_done", "task_done", 1, 1)
    #     }
    #     self.sock.sendall(pickle.dumps(pak))

    # pak = {
    #	  "type": "wait"
    # }

    # pak = {
    #	  "type": "put_ack",
    #	  "hostnames": [...]
    # }

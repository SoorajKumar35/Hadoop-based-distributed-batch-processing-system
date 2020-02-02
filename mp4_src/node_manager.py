import socket
import cPickle as pickle
# import mapjuice
import hashlib
import global_variables
import tcp_client
import os
import threading
import subprocess
import MapTask
import ReduceTask
import time


def parseIP(id):
    return id.split(":")[0]


class node_manager(threading.Thread):

    def __init__(self, port=None):
        super(node_manager, self).__init__()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if port:
            server_address = (socket.gethostname(), port)
        else:
            server_address = (socket.gethostname(), global_variables.port_map['NM'])

        self.sock.bind(server_address)
        self.sock.listen(1)

    def receive_master_cmds(self):

        print("[NM receive_master_cmds]: Launched Node Manager Listener")
        while True:
            conn, addr = self.sock.accept()
            self.master_hostname = socket.gethostbyaddr(addr[0])[0]
            print("[NM receive_master_cmds]: Received a cmd from RM")
            master_cmd = conn.recv(4096)
            master_cmd_pak = pickle.loads(master_cmd)
            args = master_cmd_pak["args"]

            if master_cmd_pak['type'] == 'map':
                shard_dir, application, prefix = tuple(args)
                hn = socket.gethostbyaddr(addr[0])[0]
                start = time.time()
                mt = MapTask.MapTask(shard_dir, application, prefix, hn, global_variables.port_map['RM'])
                print("[NM receive_master_cmds] Map Task took: ", time.time() - start, "secs")
                print("[NM receive_master_cmds]: Completed Map Task, now will partition files")
                start = time.time()
                self.partioner(prefix, '../inter_dir')
                print("[NM receive_master_cmds] Partition took: ", time.time() - start, "secs")

            elif master_cmd_pak['type'] == 'reduce':
                hn = socket.gethostbyaddr(addr[0])[0]
                inter_dir, prefix, application, dest_filename, delete = tuple(args)
                start = time.time()
                rt = ReduceTask.ReduceTask(inter_dir, prefix, application, dest_filename, delete, hn,
                                           global_variables.port_map['RM'])
                print("[NM receive_master_cmds] Reduce Task took: ", time.time() - start, "secs")
            # self.partioner(dest_filename, '../local')

            # Put output file into sdfs

            conn.close()

    def partioner(self, prefix=None, dir=None):

        # This function is used to send particular intermediate files before the Reduce
        # phase and right after the Map Phase before the reduce application is started on any
        # VM.
        if prefix and dir:
            print("[NM Partioner]: prefix = {} and dir = {}".format(prefix, dir))
            inter_files = [f for f in os.listdir(dir) if f.find(prefix) != -1]
            # print("[NM Partioner]: inter_files = {}".format(inter_files))

            self.distribute_files(dir, inter_files)

    def distribute_files(self, dir=None, files=None):

        for i, int_f in enumerate(files):
            try:
                put_file_tcp_client = tcp_client.tcp_client()
                put_file_tcp_client.connect(self.master_hostname, global_variables.port_map["sdfs"][0])
                put_file_tcp_client.req_put_file(localfilename=os.path.join(dir, int_f),
                                                 sdfsfilename=int_f)
                put_file_tcp_client.sock.close()
            except Exception, e:
                print("[NM partitioner]: Can't send file to namenode!")
                print("Error: ", e)
            os.remove(os.path.join(dir, int_f))
        print("[NM partitioner]: Completed distributing files!")

    def run(self):
        self.receive_master_cmds()


def start_node_manager():
    global_variables.local_node_manager = node_manager()
# global_variables.local_node_manager.receive_master_cmds()

# if __name__ == '__main__':
# 	nm = node_manager()

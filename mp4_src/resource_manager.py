import socket
import sys
import cPickle as pickle
import collections
import threading
import global_variables
from collections import defaultdict


class Progress(object):
    def add(self, current_file, total_files):
        self.current_file = current_file
        self.total_files = total_files


class TaskInfo(object):
    def add(self, sdfs_directory, args, type):
        self.sdfs_directory = sdfs_directory
        self.args = args
        self.type = type

def parseIP(id):
    return id.split(":")[0]

class resource_manager(threading.Thread):

    def __init__(self, port):
        """
		Initialize the resource manager or master server.
		"""

        super(resource_manager, self).__init__()

        self.job_q = []

        self.progress = defaultdict(Progress)
        self.nodes_available = collections.defaultdict(bool)
        # is the name node supposed to be available at init?
        self.nodes_available[socket.gethostname()] = True
        self.node_task_info = collections.defaultdict(TaskInfo)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server_address = (socket.gethostname(), global_variables.port_map['RM'])
        self.sock.bind(server_address)
        self.sock.listen(1)

    def receive_commands_msgs(self):
        """
        Function to receive commands from VMs to start a job or to receive message from node managers
        to receive updates on the progress made on the Map/Reduce tasks.
        """

        print("[RM receive_commands_msgs]: Receiving commands at port {}".format(global_variables.port_map["RM"]))
        while True:

            connection, client_address = self.sock.accept()
            client_address = socket.gethostbyaddr(client_address[0])[0]
            msg = connection.recv(4096)
            msg_pak = pickle.loads(msg)
            msg_tup = msg_pak["arguments"]

            if msg_pak["type"] == 'maple':

                self.unique_files_proc = set([])
                maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory = tuple(msg_tup)
                args = (sdfs_src_directory, maple_exe, sdfs_intermediate_filename_prefix)
                # Start the map phase
                if not self.job_q:
                    self.send_command('map', sdfs_src_directory, args, num_tasks=num_maples)
                else:
                    print("Map job has been queued")
                    self.job_q.append(tuple(msg_tup[1:]))

            elif msg_pak["type"] == 'juice':
                self.unique_files_proc = set([])
                juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input = tuple(
                    msg_tup)
                args = ('../sdfs', sdfs_intermediate_filename_prefix, juice_exe, sdfs_dest_filename, delete_input)
                # inter_dir, prefix, application, dest_filename, delete
                # Start the reduce phase
                self.send_command('reduce', sdfs_intermediate_filename_prefix, args, num_tasks=num_juices)

            elif msg_pak["type"] == 'progress':

                # type, done, prog_percent = msg_tup[1:]
                type, current_file, total_files = msg_tup[1:]
                self.progress[str(client_address)].current_file = current_file
                self.progress[str(client_address)].total_files = total_files

                all_current = 0.0
                all_total = 0.0
                for key in self.progress.keys():
                    # print(key, self.progress[key].current_file, self.progress[key].total_files)
                    all_current += self.progress[key].current_file
                    all_total += self.progress[key].total_files

                prog_percent = (all_current / all_total) * 100

                # print("Task from {}".format(client_address))
                print("Task Completion: {}".format(prog_percent))

                # print("all_current: ", all_current, "all_total: ", all_total)

                if all_current == all_total:
                    self.unique_files_proc = set([])
                    print("Completed  Task")
                    for client_address in self.progress.keys():
                        self.nodes_available[client_address] = True
                        self.node_task_info[client_address].args = None
                        self.node_task_info[client_address].sdfs_src_directory = None
                        self.progress[client_address].current_file = 0.0
                        self.progress[client_address].total_files = 0.0

                    if self.job_q:
                        task, sdfs_src_directory, args = self.job_q[0]
                        self.job_q = self.job_q[1:]
                        # self.send_command('map', sdfs_src_directory, args)
                        # self.send_command(type, sdfs_src_directory, args, 5)

            elif msg_pak["type"] == 'Check':
                if msg_tup not in self.unique_files_proc:
                    # print("Duplicated not detected for {}".format(msg_tup))
                    self.unique_files_proc.add(msg_tup)
                    connection.sendall('Y')
                else:
                    # print("Duplicate detected!")
                    connection.sendall('N')
            else:
                print("Unsupported message format received", msg_tup[0])

    def send_command(self, task, sdfs_src_directory, args, num_tasks):

        # Send command
        nodes_to_launch = [socket.gethostbyaddr(parseIP(member))[0] for member in global_variables.membership_list]
        nodes_to_launch = nodes_to_launch[:num_tasks]
        self.launch_process(nodes_to_launch, task=task, args=args)

    def launch_process(self, nodes_available, task=None, args=None):

        if task and args:

            if task == 'map' and args:
                pak = {
                    'type': 'map',
                    'args': args
                }

            elif task == 'reduce' and args:
                pak = {
                    'type': 'reduce',
                    'args': args
                }

            for hname in nodes_available:
                self.node_task_info[hname].args = args
                self.node_task_info[hname].sdfs_directory = args[1]
                self.node_task_info[hname].type = task

                send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                send_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                send_sock.connect((hname, global_variables.port_map['NM']))
                send_sock.sendall(pickle.dumps(pak))
                send_sock.close()
                self.nodes_available[hname] = False

        else:
            print("Please enter the correct task to launch.")

    def handle_failure(self, hostname):

        if hostname in self.node_task_info:
            print(self.node_task_info[hostname].__dict__)
            sdfs_directory = self.node_task_info[hostname].sdfs_directory
            args = self.node_task_info[hostname].args
            type = self.node_task_info[hostname].type
            print("Handling Failure at Resource manager")
            # self.send_command(type, sdfs_directory, args, num_tasks=5)
            self.job_q.append((type, args, sdfs_directory))


        else:
            print("Deleted node not used in Map tasks")

    def run(self):
        self.receive_commands_msgs()


def start_resource_manager():
    global_variables.resource_manager = resource_manager(port=global_variables.port_map['RM'])
# global_variables.resource_manager.receive_commands_msgs()

# def find_available_nodes(self, sdfs_src_directory=None, num_tasks=0):
#     """
#     Function that returns the VMs available to run a map or reduce task. Only one map or reduce
#     task is run at one time.
#     """
#
#     # Check which nodes don't currently have a process running currently on them. (Moved to self.nodes_available)
#     # Check if nodes have the files needed as input
#     if sdfs_src_directory:
#         src_files = [k for k in global_variables.file_to_machine_map.keys() if ((k.find(sdfs_src_directory) != -1))]
#         # print("[RM find_available_nodes]: sdfs_src_directory: ", sdfs_src_directory)
#         print("[RM find_available_nodes]: src_files: ", src_files)
#         # print("[RM find_available_nodes]: keys()", global_variables.file_to_machine_map.keys())
#         unique_nodes = []
#         for s in src_files:
#             # print("[RM find_available_nodes]: nodes with file", global_variables.file_to_machine_map[s])
#             curr_m = [h['hostname'] for h in global_variables.file_to_machine_map[s]]
#             unique_nodes += curr_m
#     # print("[RM find_available_nodes]: Unique nodes picked to send command to", unique_nodes)
#     else:
#         print("Please enter a source directory for the files in SDFS")
#         raise ValueError
#
#     to_ret = []
#     for hname in self.nodes_available.keys():
#         if (hname in unique_nodes) and (self.nodes_available[hname]):
#             to_ret.append(hname)
#
#     to_ret = list(set(to_ret))  # make sure its unique? -> Yes
#     print("[RM find_available_nodes]: Available nodes dict: ", self.nodes_available)
#     print("[RM find_available_nodes]: Available nodes: ", self.nodes_available.keys())
#     print("[RM find_available_nodes]: Nodes to send file to: ", to_ret)
#     print("[RM find_available_nodes]: Unique nodes: ", set(unique_nodes))
#     return to_ret[:num_tasks]
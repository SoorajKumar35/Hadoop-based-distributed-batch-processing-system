from global_variables import *
from node_manager import node_manager
from resource_manager import resource_manager, start_resource_manager
from failure_detector_functions import *
from sdfs_functions import *
from node_manager import node_manager, start_node_manager


def auto_join():
    # Join the network
    # Become the introducer if you are one of the designated introducers
    if not global_variables.master:
        # Contact the introducer and ask to join the network
        contact_introducer('fa19-cs425-g34-01')


def partition_input(localfilename):
    # num_shards = len(global_variables.membership_list)
    num_shards = 10
    filenames = []
    with open(os.path.join("../local", localfilename), 'r') as f:
        data = f.readlines()
        total_lines = len(data)
        block_size = int(total_lines / num_shards)
        blocks = [data[x:x + block_size] for x in xrange(0, total_lines, block_size)]
        for idx, block in enumerate(blocks):
            # print("Partition block: ", block)
            filename = 'shard_' + str(idx) + "_" + localfilename
            filenames.append(filename)
            with open(os.path.join("../local", filename), 'w') as wf:
                for line in block:
                    wf.write(line + '\n')
                wf.close()
        f.close()
    # os.remove(os.path.join("../local", localfilename))
    return filenames


def command_loop(sock):
    auto_joined_network = False

    for file in os.listdir("../sdfs"):
        os.remove(os.path.join("../sdfs", file))

    while not global_variables.exiting:

        # Auto-join the first VM
        if (not global_variables.master) and (not auto_joined_network):
            auto_joined_network = True
            auto_join()

        try:
            line = raw_input("Please enter a command: ")
        except ValueError:
            continue
        except NameError:
            continue
        except SyntaxError:
            continue

        line = line.split(" ")

        if line[0] == "ID":

            # Get and print the ID:
            print(get_id())

        elif line[0] == "print":

            # print the membership list
            print_membership_list()

        elif line[0] == "print_file_to_machine_map":
            for item in global_variables.file_to_machine_map.items():
                print(item)

        elif line[0] == "join":
            for file in os.listdir("../sdfs"):
                os.remove(os.path.join("../sdfs", file))
            # Join the network
            # Become the introducer if you are one of the designated introducers
            if not global_variables.master:
                # Contact the introducer and ask to join the network
                contact_introducer(line[1])

        elif line[0] == "leave":

            # Leave the network
            command = "delete:" + str(get_id())
            dissimenate(command)

            # reset member list
            init_member_list()

        elif line[0] == "exit":

            sock.close()
            global_variables.exiting = True
            send_packet("exit", "127.0.0.1", global_variables.port)
            print("Closed socket and binded conn to port. Exiting....")
            return

        elif line[0] == "reset":
            global_variables.bandwidth_usage = 0
            global_variables.start_time = datetime.now()

        elif line[0] == "put":
            start = time.time()
            # Execute a put command to the SDFS file system
            # put localfilename sdfsfilename
            name_node_hostname = socket.gethostbyaddr(parseIP(global_variables.membership_list[0]))[0]

            if len(line) > 3:
                if line[3] == '-p':
                    block_fns = partition_input(line[1])
                    print("[CMD line]: block_fns", block_fns)
                    for i, shard_f in enumerate(block_fns):
                        pclient = tcp_client()
                        try:
                            pclient.connect(name_node_hostname, global_variables.port_map['sdfs'][0])
                        except Exception, e:
                            print("Can't connect to namenode! Please try again... ")
                            print("Error: ", e)
                            continue

                        print("Sending out: ", shard_f, "SDFS fn: ", line[2] + '_' + str(i) + '.txt')
                        pclient.req_put_file(shard_f, line[2] + '_' + str(i) + '.txt')
            else:
                client = tcp_client()
                try:
                    client.connect(name_node_hostname, global_variables.port_map['sdfs'][0])
                except Exception, e:
                    print("Can't connect to namenode! Please try again... ")
                    print("Error: ", e)
                    continue
                client.req_put_file(line[1], line[2])

            # print("duration: ", time.time() - start)

        elif line[0] == "cat":
            filepath = os.path.join('../sdfs', line[1])
            subprocess.Popen(['cat', filepath])

        elif line[0] == "get":
            start = time.time()
            # get sdfsfilename localfilename
            name_node_hostname = parseIP(global_variables.membership_list[0])
            client = tcp_client()
            client.connect(name_node_hostname, global_variables.port_map['sdfs'][0])
            client.get_file(line[1], line[2])
            filepath = os.path.join('../local', line[2])
            # print("Size: ", os.path.getsize(filepath))
            subprocess.Popen(['cat', filepath])
            # print("Duration: ", time.time() - start)

        elif line[0] == "delete":
            # delete sdfsfilename
            name_node_hostname = parseIP(global_variables.membership_list[0])
            client = tcp_client()
            client.connect(name_node_hostname, global_variables.port_map['sdfs'][0])
            client.req_delete(line[1])

        elif line[0] == "ls":
            # ls sdfsfilename
            name_node_hostname = parseIP(global_variables.membership_list[0])
            client = tcp_client()
            client.connect(name_node_hostname, global_variables.port_map['sdfs'][0])
            client.req_ls(line[1])
            for hostname in pickle.loads(client.recv()):
                print(socket.gethostbyaddr(parseIP(hostname))[0])

        elif line[0] == "store":
            for file in os.listdir("../sdfs"):
                print(file)

        elif line[0] == "maple":
            maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory = line[1:]
            master_hostname = socket.gethostbyaddr(parseIP(global_variables.membership_list[0]))[0]
            client = tcp_client()
            client.connect(master_hostname, global_variables.port_map['RM'])
            client.send_maple_command(maple_exe, int(num_maples), sdfs_intermediate_filename_prefix, sdfs_src_directory)
            client.sock.close()

        elif line[0] == "juice":
            print("Line: ", line[1:])
            juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input = line[1:]
            master_hostname = socket.gethostbyaddr(parseIP(global_variables.membership_list[0]))[0]
            client = tcp_client()
            client.connect(master_hostname, global_variables.port_map['RM'])
            client.send_juice_command(juice_exe, int(num_juices), sdfs_intermediate_filename_prefix, sdfs_dest_filename,
                                      int(delete_input))
            client.sock.close()


def main():
    global_variables.exiting = False
    global_variables.start_time = datetime.now()

    init_member_list()
    with heartbeat_lock:
        global_variables.heartbeat = defaultdict(bool)

    # Initialize the filename to hostname map
    global_variables.file_to_machine_map = defaultdict(list)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    global_variables.port_map = {
        "RM": 9898,
        "NM": 10000,
        "sdfs": [5454],
        "heartbeat": 1337
    }

    # Initialize node manager

    # Spawn listening task
    threading.Thread(target=listen_for_hearbeat_and_disMsgs, args=(sock,)).start()
    threading.Thread(target=send_heartbeats).start()
    threading.Thread(target=heartbeat_failure_detector).start()

    threading.Thread(target=tcp_command_recieve, args=(global_variables.port_map["sdfs"][0],)).start()


    global_variables.local_node_manager = node_manager(port=global_variables.port_map['NM'])
    global_variables.local_node_manager.deamon = True
    global_variables.local_node_manager.start()

    if (global_variables.master):
        global_variables.resource_manager = resource_manager(port=global_variables.port_map['RM'])
        global_variables.resource_manager.deamon = True
        global_variables.resource_manager.start()

    command_loop(sock)


if __name__ == "__main__":
    global_variables.timeOfEntry = time.time()
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-i', action='store_true')
    parser.add_argument('-p', type=int, required=False, default=1337)
    args = parser.parse_args()
    global_variables.master = args.i
    global_variables.port = args.p
    assert (global_variables.master != None)

    main()

import socket
import argparse
import sys
import os
from tcp_client import tcp_client
import cPickle as pickle


def main():
    if not os.path.exists('sdfs'):
        os.makedirs('sdfs')
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=int)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.bind(('localhost', args.p))
    sock.listen(1)

    while True:
        (client_sock, address) = sock.accept()
        hostname = socket.gethostbyaddr(address[0])[0]
        print(hostname+" connected") 
        client = tcp_client(client_sock)
        resp = pickle.loads(client.recv())
        print(resp)
        if resp['type'] == 'download':
            client.resp_download_file(resp['filename'])
        elif resp['type'] == 'put':
            # # this is assuming we accept the put and is coming from name node
            # # this is happening on the name node
            # client.resp_put_file_ack(resp['filename'])
            # # this is happening on the data node
            # resp = pickle.loads(client.recv())
            # print(resp)
            # if resp['type'] == 'put_file':
            #     data = client.recv()
            #     with open(os.path.join('sdfs', resp['filename']), 'wb') as f:
            #         f.write(data)

            # assuming we want to wait (namenode)
            filename = resp['filename']
            client.resp_put_file_wait(filename)
            resp = client.recv()
            print(resp)
            if resp == "yes":
                client.resp_put_file_ack(filename)
                # this is happening on the data node
                resp = pickle.loads(client.recv())
                print(resp)
                if resp['type'] == 'put_file':
                    data = client.recv()
                    with open(os.path.join('sdfs', resp['filename']), 'wb') as f:
                        f.write(data)



        client.sock.close()


    

if __name__ == "__main__":
    main()

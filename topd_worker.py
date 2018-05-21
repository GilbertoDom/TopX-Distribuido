#! /usr/bin/env python3
# -*- coding:utf-8 -*-

"""
Universidad Nacional Autonoma de Mexico
Escuela Nacional de Estudios Superiores
Licenciatura en Tecnologias para la Informacion en Ciencias
Materia: Computo Distribuido
Titulo: Proyecto 2, Top X: Yahoo Dataset
Autor(es): Gilberto Carlos Dominguez Aguilar, Misael Centeno Olivares
"""

import argparse
import socket
from time import sleep


def client(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    ok, not_cool = ok_greet(sock)
    if not_cool:
        print('Something wrong, bye...')
        sock.close()
    else:
        print('Server Response: ')
        print(ok.decode('ascii'))
        print()


        print('Ready to receive...')
        print()
        #sleep(1)

        data = recvall(sock)
        print(len(data))

        #receive_data(sock)
    #else:
    #    print('someting wrong...')
    #    print('closing...')
    #    sock.close()
    print('finished')
    print('Bye.')
    sock.close()


def ok_greet(sock):
    not_cool = False
    res = b''
    try:
        length = sock.recv(2)
        res = sock.recv(int(length))
        # sock.sendall(greet.encode())

    except Exception as err:
        print('encountered error: {} while greeting '.format(err))
        not_cool = True
    return res, not_cool


def end_data(line):
    end = False
    # print('s')
    if b'END' in line:
        #print(chunk)
        end = True
    return end


def recvall(sock):
    data = b''
    try:
        done = False
        while not done:
            length = sock.recv(2)
            length = int(length)
            line = b''
            while len(line) < length:
                more = sock.recv(length - len(line))
                if not more:
                    raise EOFError('was expecting {} bytes but only received'
                                   ' {} bytes before the socket closed'.format(
                                   length, len(data)))
                line += more
                data += line
                if end_data(line):
                    done = True
                    break

    except Exception as err:
        print('Error while receiving data: {}'.format(err))
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Example client')
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    args = parser.parse_args()
    address = (args.host, args.p)
    client(address)

#EOF
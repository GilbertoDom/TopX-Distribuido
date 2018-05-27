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
import struct

header_struct = struct.Struct('!I')  # messages up to 2**32 - 1 in length

DATA = []


def recvall(sock, length):
    blocks = []
    while length:
        block = sock.recv(length)
        if not block:
            raise EOFError('socket closed with {} bytes left'
                           ' in this block'.format(length))
        length -= len(block)
        blocks.append(block)
    return b''.join(blocks)

def get_block(sock):
    data = recvall(sock, header_struct.size)
    (block_length,) = header_struct.unpack(data)
    data = recvall(sock, block_length)
    return data.decode('ascii')

def put_block(sock, message):
    encoded_message = message.encode('ascii')
    block_length = len(encoded_message)
    sock.send(header_struct.pack(block_length))
    sock.send(encoded_message)


def client(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    ok, not_cool = ok_greet(sock)
    if not_cool:
        print('Something wrong, bye...')
        sock.close()
    else:
        print('Server Response: ')
        print(ok)
        print()

        print('Ready to receive...')
        print()

        lines_received = 0
        data = get_block(sock)
        while not end_data(data):
            lines_received += 1
            if lines_received % 10000 == 0:
                print("\rLines received: {:,}".format(lines_received), end='')

            l = [int(x) for x in data.rstrip().split('\t')]
            DATA.append(l)

            data = get_block(sock)

        print()
        print('Now receiving additional files...')

        #f1 = ''
        #data = get_block(sock)
        #while not end_data()




    #else:
    #    print('someting wrong...')
    #    print('closing...')
    #    sock.close()
    print('Finished')
    print('Bye.')
    sock.close()


def ok_greet(sock):
    not_cool = False
    res = ''
    try:
        res = get_block(sock)
    except Exception as err:
        print('encountered error: {} while greeting '.format(err))
        not_cool = True
    return res, not_cool


def end_data(line):
    end = True if line == 'END' else False
    return end


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Example client')
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    args = parser.parse_args()
    address = (args.host, args.p)
    client(address)

#EOF
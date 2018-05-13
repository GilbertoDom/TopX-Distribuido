#! /usr/bin/env python3
#-*- coding:utf-8 -*-
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


def client(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    if ok_greet(sock).startswith(b'o'):
        #print(ok_greet(sock))
        print('Ready to receive...')
        print()

        #receive_data(sock)
    else:
        print('someting wrong...')
        print('closing...')
        sock.close()
    print('finished')
    print('Bye.')
    sock.close()


def ok_greet(sock):
    greet = 'ALL OK\n'
    data = b''
    try:
        sock.sendall(greet.encode())
        data = recv_until(sock, b'\n')
    except Exception as err:
        print('encountered error: {} while greeting '.format(err))
    return data


def receive_data(sock):
    data = ''
    n = 0
    b_count = 0
    while True:
        chunk = recv_until(sock, b'\n')
        end = end_data(chunk)
        if not end:
            n += 1
            b_count += len(chunk)
            data += chunk
            continue
        else:
            print('received {} lines with a total of {} bytes.'.format(n, b_count))
            break

    return data


def end_data(chunk):
    s = chunk.decode('ascii')


    return end


def recv_until(sock, suffix):
    """Receive bytes over socket `sock` until we receive the `suffix`."""
    message = sock.recv(4096)  # arbitrary value of 4KB
    if not message:
        raise EOFError('socket closed')
    while not message.endswith(suffix):
        data = sock.recv(4096)
        if not data:
            raise IOError('received {!r} then socket closed'.format(message))
        message += data
    return message

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Example client')
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    args = parser.parse_args()
    address = (args.host, args.p)
    client(address)

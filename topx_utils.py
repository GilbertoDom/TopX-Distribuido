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

import asyncio
import argparse, sys, zen_utils
from os import listdir
from os.path import isfile, join
from pathlib import Path

_PATH_ = "/home/alumnos/PycharmProjects/topX"
_DATAPATH_ = "{}/ydata-ymusic-user-song-ratings-meta-v1_0/".format(_PATH_)
k = 0
n_conn = 0
k_workers = {}


@asyncio.coroutine
def handle_conversation(reader, writer):
    global k, k_workers, n_conn
    address = writer.get_extra_info('peername')
    print('Accepted connection from {}'.format(address))
    n_conn += k
    k_workers[n_conn] = address
    print('{} worker(s) connected, waiting for {} more...'.format(n_conn, k - n_conn))
    if n_conn == k:
        while True:
            data = b''

            while not data.endswith(b'\n'):
                more_data = yield from reader.read(4096)
                if not more_data:
                    if data:
                        print('Client {} sent {!r} but then closed'
                              .format(address, data))
                    else:
                        print('Client {} closed socket normally'.format(address))
                    return
                data += more_data
                print(data)
            #for i in range(k):
            yield from send_to(writer, data)



    n_conn = 0
    print('connection closed with {}'.format(address))


def parse_command_line(description):
    """Parse command line and return a socket address."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    parser.add_argument('-m', help="Will execute m algorithm, so you might as well choose an output filename.",
                        default="topGenres")
    parser.add_argument('-n', help="Will execute n algorithm, so you might as well choose an output filename.",
                        default="topSongs")
    parser.add_argument('-k', type=int, help="number of workers (nodes).", default=2)
    args = parser.parse_args()
    address = (args.host, args.p)

    return address, args.k


#@asyncio.coroutine
async def send_to(writer, data):
    answer = b'ok\n'
    writer.write(answer)




def read_f(filename):
    with open(filename) as f:
        for line in f:
            yield line


def list_files(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    l = []
    for f in onlyfiles:
        if f.endswith('.txt'):
            l.append(f)
    return l


"""
class TServer(asyncio.Protocol):

    def __init__(self, k):
        self.k = k
        self.n_conn = 0
        self.k_workers = {}


    def connection_made(self, transport):
        self.transport = transport
        self.address = transport.get_extra_info('peername')
        print('Accepted connection from {}'.format(self.address))
        self.n_conn += 1
        self.k_workers[self.n_conn] = self.address
        print('{} worker(s) connected, waiting for {} more...'.format(self.address, self.k - self.n_conn))

        if self.n_conn == self.k:

            self.send_files(self.transport)

    def data_received(self, data):
        self.data += data

    def connection_lost(self, exc):
        if exc:
            print('Client {} error: {}'.format(self.address, exc))
        elif self.data:
            print('Client {} sent {} but then closed'
                  .format(self.address, self.data))
        else:
            print('Client {} closed socket'.format(self.address))

    def send_files(self, transport):

        avl_f = list_files(_PATH_)

"""

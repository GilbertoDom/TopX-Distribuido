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
import argparse
from os import listdir
from os.path import isfile, join
from time import sleep


_PATH_ = "/home/alumnos/PycharmProjects/topX"
# _DATAPATH_ = "{}/ydata-ymusic-user-song-ratings-meta-v1_0/".format(_PATH_)
_DATAPATH_ = "{}/data/".format(_PATH_)

k = None
n = 0
workers = {}
transports = {}


async def handle_conversation(reader, writer):
    global workers, transports, k, n
    address = writer.get_extra_info('peername')
    print('Accepted connection from {}'.format(address))
    if address not in workers.values():
        n += 1
        workers[n] = address
        transports[address] = (reader, writer)
        print('{} worker(s) connected, waiting for {} more...'.format(n, k - n))
        await send_hello(writer)

    if n == k:
        await send_data(k)

        print('Data successfully sent to workers.')
        print('wait for response...')

        #await receiving()
        print('Done')
    # print('connection closed with {}'.format(address))


async def send_data(k):

    print('Preparing to send data...')

    bcount = 0

    l_of_files = list_files(_DATAPATH_)
    for f in l_of_files:
        dat = readd(f)
        i = 1

        for line in dat:
            address = workers[i]
            writer = transports[address][1]
            length = str(len(line))
            bcount += len(line.encode())
            writer.write(length.encode())
            writer.write(line.encode())
            i += 1
            if i > k:
                i = 1

    print('Done, now sending END notice to workers...')

    for add in workers.values():
        writer = transports[add][1]
        end = b'END'
        l = str(len(end))
        writer.write(l.encode())
        writer.write(end)
        print('Done. END notice sent to {}.'.format(add))

    print('A total of {} bytes where sent.'.format(bcount))
    print()


async def send_hello(writer):
    answer = b'HELLO CLIENT'
    length = str(len(answer))
    writer.write(length.encode())
    writer.write(answer)


def readd(filename):
    with open(_DATAPATH_+filename) as f:
        for line in f:
            yield line


def list_files(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    l = []
    for f in onlyfiles:
        if f.endswith('.txt'):
            if f.startswith('t'):
                l.append(f)
    return l


def parse_command_line(description):
    """Parse command line and return a socket address."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    parser.add_argument('-m', help="Will execute m algorithm.",
                        default="topGenres")
    parser.add_argument('-n', help="choose an output filename.",
                        default="topSongs")
    parser.add_argument('-k', type=int, help="number of workers (nodes).", default=2)
    args = parser.parse_args()
    address = (args.host, args.p)

    return address, args.k

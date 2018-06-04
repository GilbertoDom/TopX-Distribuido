#! /usr/bin/env python3
# -*- coding:utf-8 -*-

"""
Universidad Nacional Autonoma de Mexico
Escuela Nacional de Estudios Superiores
Licenciatura en Tecnologias para la Informacion en Ciencias
Materia: Computo Distribuido
Titulo: Proyecto 2, Top X: Yahoo Dataset
Autor(es): Gilberto Carlos Dominguez Aguilar
"""


import argparse
import struct
from os import listdir
from os.path import isfile, join
from ast import literal_eval as make_tuple
import operator


_PATH_ = "/home/alumnos/PycharmProjects/topX"
_DATAPATH_ = "{}/ydata-ymusic-user-song-ratings-meta-v1_0/".format(_PATH_)

k = None
n = 0
workers = {}
transports = {}
results = []

header_struct = struct.Struct('!I')  # messages up to 2**32 - 1 in length


async def recvall(reader, length):
    blocks = []
    while length:
        block = await reader.read(length)
        if not block:
            raise EOFError('socket closed with {} bytes left'
                           ' in this block'.format(length))
        length -= len(block)
        blocks.append(block)
    return b''.join(blocks)


async def get_block(reader):
    data = await recvall(reader, header_struct.size)
    (block_length,) = header_struct.unpack(data)
    data = await recvall(reader, block_length)
    return data.decode('ascii')


async def put_block(writer, message):
    encoded_message = message.encode('ascii')
    block_length = len(encoded_message)
    writer.write(header_struct.pack(block_length))
    writer.write(encoded_message)


async def handle_conversation(reader, writer):
    global workers, transports, k, n, results
    address = writer.get_extra_info('peername')

    if address not in workers.values():
        print('Accepted connection from {}'.format(address))
        n += 1
        workers[n] = address
        transports[address] = (reader, writer)
        print('{} worker(s) connected, waiting for {} more...'.format(n, k - n))
        await send_hello(writer)

        if n == k:
            print('Preparing to send data...')
            await send_data(k)

            print('Data successfully sent to workers.')
            print('Now sending additional files...')

            await additional()

            print('Waiting for response...')

            i = 0
            for transp in transports.values():
                lector = transp[0]
                response = ''
                data = await get_block(lector)
                while not end(data):
                    response += data
                    data = await get_block(lector)
                results.append(response)
                i += 1
                print('Received {} response(s)...'.format(i))

            print('All responses received.')
            top_songs, top_gens = final_result(results)
            with open('final_results.txt', 'w') as f:
                for song in top_songs:
                    f.write(str(song))
                    f.write('\t')

                f.write('\n')

                for gen in top_gens:
                    f.write(str(gen))
                    f.write('\t')
            print('Results ready.')

    print('Done')



def final_result(results):
    Score = []
    final = {}
    countdown = []
    gen = []
    for result in results:
        l = result.split('\n')
        countdown += l[0].split('\t')
        gen += l[1].split('\t')

    for t in countdown:
        song = make_tuple(t)
        id, rate = song[0], song[1]
        if id not in final.keys():
            final[id] = rate
        else:
            final[id] += rate

    for i in range(10):
        top_rated = max(final.items(), key=operator.itemgetter(1))[0]
        Score.append(top_rated)
        del final[top_rated]

    top_g = set(gen)

    return Score, top_g


async def send_data(k):

    print('sending data...')

    bcount = 0
    l_of_files = list_files(_DATAPATH_)
    for f in l_of_files:
        dat = readd(f)
        i = 1

        for line in dat:
            address = workers[i]
            writer = transports[address][1]
            await put_block(writer, line)
            bcount += len(line.encode('ascii'))
            i += 1
            if i > k:
                i = 1

    print('Done, now sending END notice to workers...')

    for add in workers.values():
        writer = transports[add][1]
        await send_EOT(writer)
        print('Done. END notice sent to {}.'.format(add))

    print('A total of {} bytes where sent.'.format(bcount))
    print()


async def send_EOT(writer):
    await put_block(writer, 'END')


async def send_hello(writer):
    await put_block(writer, 'HELLO CLIENT')


def readd(filename):
    with open(_DATAPATH_+filename) as f:
        for line in f:
            yield line


def list_files(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    l = []
    for f in onlyfiles:
        if f.endswith('.txt'):
            if f.startswith('t'): # SRTM: Modified to test with only 1 file
                l.append(f)
    return l


async def additional():
    to_send = ['genre-hierarchy.txt',
               'song-attributes.txt']
    for f in to_send:
        dat = readd(f)
        for line in dat:
            for transp in transports.values():
                writer = transp[1]
                await put_block(writer, line)

        for transp in transports.values():
            writer = transp[1]
            await send_EOT(writer)

    print('Addtitional files sent to workers.')


def end(data):
    end = True if data == 'END' else False
    return end


def parse_command_line(description):
    """Parse command line and return a socket address."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    parser.add_argument('-m', help="m algorithm.",
                        default="topGenres")
    parser.add_argument('-n', help="choose an output filename.",
                        default="topSongs")
    parser.add_argument('-k', type=int, help="number of workers (nodes).", default=2)
    args = parser.parse_args()
    address = (args.host, args.p)

    return address, args.k

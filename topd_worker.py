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
import operator

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
            DATA.append((l[1], l[2]))

            data = get_block(sock)

        print()
        print('Now receiving additional files...')

        song_map, genre_map = additional_files(sock)

        print('Now getting top 10 songs...')
        songs = get_top_songs(DATA)

        print('Done.')

        print('Now getting top 10 genres...')
        genres = get_top_genres(songs, song_map, genre_map)
        #print(genres)
        print('Done.')

        print('Results ready, now sending to master...')
        send_results(sock, songs, genres)



    #else:
    #    print('someting wrong...')
    #    print('closing...')
    #    sock.close()
    print('Finished')
    print('Bye.')
    sock.close()


def send_results(sock, songs, genres):
    song = '\t'.join([str(x) for x in songs])
    song += '\n'
    gen = '\t'.join([str(x) for x in genres])
    gen += '\n'
    message = song+gen
    put_block(sock, message)
    put_block(sock, 'END')

    print('Results sent.')


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


def additional_files(sock):
    song_to_genre = {}
    genrename = {}
    for i in range(2):
        lines = 0
        data = get_block(sock)
        while not end_data(data):
            lines += 1
            if lines % 10000 == 0:
                print("\rAdditional lines received: {:,}".format(lines), end='')

            l = data.rstrip().split('\t')

            if i == 0:
                genre_id = l[0]
                genre_name = l[3]
                genrename[int(genre_id)] = genre_name

            else:
                song_id = l[0]
                genre_id = l[3]
                song_to_genre[int(song_id)] = int(genre_id)

            data = get_block(sock)

    print()
    print('Files received')
    print('Data ready...')

    return song_to_genre, genrename


def get_top_songs(DATA):
    top = {}
    Top = []
    for line in DATA:
        song = line[0]
        rate = line[1]
        if song not in top.keys():
            top[song] = rate
        else:
            top[song] += rate
    for i in range(10):
        top_rated = max(top.items(), key=operator.itemgetter(1))[0]
        Top.append((top_rated, top[top_rated]))
        del top[top_rated]

    return Top


def get_top_genres(song_list, songs_map, genre_map):
    top = []
    for song in song_list:
        genre_id = songs_map[song[0]]
        genre_name = genre_map[genre_id]
        top.append(genre_name)
    return top


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Example client')
    parser.add_argument('host', help='IP or hostname')
    parser.add_argument('-p', metavar='port', type=int, default=1060,
                        help='TCP port (default 1060)')
    args = parser.parse_args()
    address = (args.host, args.p)
    client(address)

#EOF
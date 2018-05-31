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

import asyncio
import topx_utils


if __name__ == '__main__':
    address, k = topx_utils.parse_command_line('asyncio server using coroutine')
    topx_utils.k = k
    loop = asyncio.get_event_loop()
    #loop.set_debug(1)
    coro = asyncio.start_server(topx_utils.handle_conversation, *address, reuse_port=True)
    server = loop.run_until_complete(coro)
    print('Listening at {}'.format(address))
    try:
        loop.run_forever()
    finally:
        server.close()
        loop.close()
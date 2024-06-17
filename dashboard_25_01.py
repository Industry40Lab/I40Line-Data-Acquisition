import logging
import asyncio
import random
import time
import json
import datetime
from functools import partial
import pandas as pd
from owlready2 import *
from asyncua import Client, Node, ua
import aiofiles

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger('asyncua')
#----------------------------------------------------------------------------

def ser_json(number):
    in_dict = {}
    in_dict['values'] = number
    json_obj = json.dumps(in_dict)
    #encode e decode servono per convertire stringe in bytes e viceversa
    return json_obj.encode('utf-8')

async def handle_command(queue, reader, writer):
    #Il reader permette di leggere quello che il client manda,
    #mentre il writer permette di inviare al client dei bytes

    ready = False
    message = ""
    while not ready:
        data = await reader.read(100)
        message += data.decode()
        addr = writer.get_extra_info('peername')
        #Questo elemento serve per leggere l'indirizzo del client
        #che è in connessione

        if data[-1] == 10:
            print(f"Received {message!r} from {addr!r}")
            ready = True

    print(f"{message}")

    if message[0:8] == "Send Max":
        number = [20 for i in range(20)]
    else:
        number = []
        for i in range(20):
            if queue.empty():
                break
            else:
                p_tuple = await queue.get()
                station, node, dt, var = p_tuple
                number.append({"station":station, "node":node, "timestamp":dt, "value":var})
                queue.task_done()
                #con get ottieni un valore e con done elimini 
                #il valore che hai ottenuto dalla coda

    data = ser_json(number)
    print(f"Send: {number}")
    writer.write(data)
    await writer.drain()

    print("Close the connection")
    writer.close()

async def start_comm(queue):
    hand = partial(handle_command, queue)
    server = await asyncio.start_server(
        hand, '127.0.0.1', 8888)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()


#----------------------------------------------------------------------------

def init_opc():
    """
    Read from conf.json file the configuration
    """

    with open(r'conf.json',"r") as f:
        json_text = f.read()
       
    parsed = json.loads(json_text)

    return parsed

async def producer(name, queue, conf, delay):
    print("Entrato in producer", name)
    # Wait delay seconds before every read
    #----------------------------------------------------------------------
    ## Sleep accordingly to reading frequency
    #await asyncio.sleep(delay)
    p_tuple= None
    print(f"opcuaddress={conf['opcuaddress']}")
    async with Client(url=conf['opcuaddress']) as client:
        #_logger.info('Children of root are: %r', await client.nodes.root.get_children())
        while True:
            for i in range(len(conf['nodeids'])):
                node_name = conf['nodeids'][i]['node_name']
                node_id = conf['nodeids'][i]['node_id']
                print(f"node_name={node_name}")
                print(f"node_id={node_id}")
                #-----------------------------------------------------------------------------------------------------------------------------------
                #-----------------------------------------------------------------------------------------------------------------------------------
                var = client.get_node(node_id)
                #-----------------------------------------------------------------------------------------------------------------------------------
                #-----------------------------------------------------------------------------------------------------------------------------------
                #_logger.info("My variable", var)
                var_value = await var.read_value() 
                #var_value = -1
                print("-------------------------------------------------------------------------------------------------------------------------------",type(var_value))
                #_logger.info("My variable value", str(var_value))
                p_tuple = (name,
                           f"{node_name}",
                           str(datetime.datetime.now()),
                           var_value)
                print ("-------------------------------------------------------------------------------------------------------------------",p_tuple)
                queue.put_nowait(p_tuple)
                #_logger.info(f"Producer -> Reception completed {name} {conf['opcuaddress']}"i)
            
            await asyncio.sleep(delay)

async def as_main(conf):
    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    # Stations to read

    stations = [
                'FrontCoverPLC',
                'DrillPLC',
                'RobotPLC',
                'CameraPLC',
                'BackCoverPLC',
                'PressPLC',
                'ManualPLC',
                'FrontCoverEnergy',
                'DrillEnergy',
                'RobotEnergy',
                'CameraEnergy',
                'BackCoverEnergy',
                'PressEnergy',
                'ManualEnergy'
                ]
    #nodes = [
    #        'xBG1',
    #        ]

    stat_par = {}
    for el in conf:
        if el['station_name'] in stations:
            print("***",el['station_name'])
            stat_par[el['station_name']] = el.copy()
            stat_par[el['station_name']]['nodeids'] = []
            for el1 in el['nodeids']:
                # ATTENZIONE questo selezione solo xBG1
                #if el1['node_name'] in nodes:
                #    stat_par[el['station_name']]['nodeids'].append(el1)
                stat_par[el['station_name']]['nodeids'].append(el1)
                print(el['station_name'], el1['node_name'])

    _logger.info(f'Stat_par len= {len(stat_par)}')
    #
    # Producer tasks
    #
    p_tasks = set()
    c_tasks = set()
    #----------------------------------------------------------------------
    # Create producers tasks to read the remote servers concurrently.
    for key in stat_par.keys():
        #Le chiavi sono solo il nome delle stazioni
        task = asyncio.create_task(producer(f'{key}', queue, stat_par[key], 0.2))
        p_tasks.add(task)
    
        # The callback delete entry in p_tasks when producer is terminated
        task.add_done_callback(p_tasks.discard)
        #Quando ha finito la task questa viene eliminata da p_task
    
        _logger.info(f'Producer={key} created')
    
    #----------------------------------------------------------------------
    task = asyncio.create_task(start_comm(queue))
    c_tasks.add(task)

    # The callback delete entry in p_tasks when producer is terminated
    task.add_done_callback(c_tasks.discard)

    _logger.info(f'Interprocess Comm created')
    
    # end old routine
    #
    # Create Consumer task
    #task = asyncio.create_task(consumer(f'consumer', queue, onto, TS, rep_queue, dis_queue, f_reasoner))
    #a_tasks.add(task)
    #_logger.info('Consumer created')

    # Wait for producer(s) termination len(p_tasks) == 0
    started_at = time.monotonic()
    while (len(p_tasks) > 0) or (not queue.empty()):
        await asyncio.sleep(2)
    
    # Cancel comm. task
    for ts in c_tasks:
        try:
            ts.cancel()
        except Exception:
            pass

    _logger.info(f'Producer(s) completed Len(p_tasks)= {len(p_tasks)}')
    _logger.info(f'Queue Empty')
    _logger.info(f'Intercomm completed')
    _logger.info(f'Tasks completed...')

    total_slept_for = time.monotonic() - started_at

    _logger.info(f'Total_slept = {total_slept_for}')

def main():
    """
       Main: make initial configurations,
             start async main loop - as_main -
    """
    #
    # Load remote server access parameters
    #
    conf = init_opc()
    _logger.info("OPC configuration loaded")

    #
    # execute main async loop
    #
    asyncio.run(as_main(conf), debug=True)

    return

if __name__ == "__main__":
    #
    # Execute main if not loaded ad a python module
    main()

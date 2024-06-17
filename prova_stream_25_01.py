import asyncio
import random
import time
import json
import streamlit as st
import pandas as pd
import csv

df = pd.DataFrame({})

async def send_command(time_stamp, command):

    reader, writer = await asyncio.open_connection(
        '127.0.0.1', 8888)

    # IMPORTANTE -> Terminare sempre il comando con \n
    #               accertarsi che in ciò che si spedisce
    #               non sia compreso un \n spurio che deve
    #               servire solo come terminatore.
    message = f"{command} {str(time_stamp)}\n"

    #print(f'Send: {message!r}')
    writer.write(message.encode())
    #Questo passaggio è necessario per comunicare con il server
    await writer.drain()

    data = await reader.read()
    json_obj = json.loads(data.decode('utf-8'))
    
    #Prima un comando viene inviato con il writer, queso comando viene letto dal reader del server e
    #convertito in stringa per poi essere riconvertito in json nel server

    #print(f"Received: {json_obj['values']}")
    writer.close()

    print('Close the connection')
    return json_obj['values']

def get_values(time_stamp):
    lista = asyncio.run(send_command(time_stamp, command='send'))
    return lista

def converter (lista):
    dictionary = []
    for el in lista:
        for key, value in el.items():
            el[key] = value
        dictionary.append(el)
    return dictionary

def main():
    global df
    while True:
        tmpTS = pd.Timestamp.now()
        time_stamp = str(tmpTS)
        lista = get_values(time_stamp)
        #print (lista)
        list_dict = converter(lista)
        df1 = pd.DataFrame.from_dict(list_dict)
        df = pd.concat([df,df1], ignore_index = True)
        print (len(df))
        if len(df) >= 80000:
            break
    df.to_csv('data_collection.csv')

main()

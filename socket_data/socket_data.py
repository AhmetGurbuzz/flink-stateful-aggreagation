#!/usr/bin/env python3

import socket
import time
import sys
from random import randint

HOST = '127.0.0.1'
PORT = 9999

time_sleep = 1

try:
    data_rate = int(sys.argv[1])
except:
    sys.exit("Check sys.argv[1] argument: <data_rate(row/s)>, try to give integer value")

time_sleep = time_sleep / data_rate


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    print("Start Flink Job...")
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            #                        CUSTOMER_ID          TRX_AMOUNT         UXIXTIME
            packet = b'%d %d %d\n' % (randint(1,5000000), randint(100,1000), randint(1606000497,1606302497))
            conn.sendall(packet)
            time.sleep(time_sleep)
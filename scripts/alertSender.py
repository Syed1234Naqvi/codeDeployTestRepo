#!/usr/bin/python

import socket
import os
import subprocess

filename = 'alertsRecords.txt'
TCP_IP = '0.0.0.0'
TCP_PORT = 5005
BUFFER_SIZE = 20  # Normally 1024, but we want fast response

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

conn, addr = s.accept()
print 'Connection address:', addr
while 1:
	data = conn.recv(BUFFER_SIZE)
	if not data: break
	print "received data:", data
	data = "message reached server"
	if os.stat(filename).st_size != 0:
		conn.send(subprocess.check_output(['tail','-1',filename])[0:-1])

conn.close()

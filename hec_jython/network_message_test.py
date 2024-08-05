#!/bin/env python
import errno
import socket
import sys
import threading
import time

request_shutdown     = "REQUEST_SHUTDOWN"
request_messages     = "REQUEST_MESSAGES:"
messages_sent        = "MESSAGES_SENT:"
message_request_tmpl = "%s%%d:%%d:%%d:%%d" % request_messages
messages_sent_tmpl   = "%s%%d" % messages_sent 

def server(port) :
	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.bind((socket.gethostname(), port))
	print("Listening on port %d" % port)
	server_sock.listen(0) # just one client at a time
	while True :
		(ctl_sock, client_ctl_address) = server_sock.accept()
		client_host = client_ctl_address[0]
		msg = ctl_sock.recv(4096)
		if msg == request_shutdown :
			print("Received shutdown request")
			server_sock.close()
			return
		if not msg.startswith(request_messages) :
			raise ValueError("Unexpected message from %s: %s" % (client_address, msg))
		try :
			message_count, data_port, block_size, wait_millis = list(map(int, msg.split(":")[1:]))
			if message_count == 0 :
				server_sock.close()
				return
		except :
			raise ValueError("Unexpected message from %s: %s" % (client_address, msg))
		if block_size and wait_millis :
			print("Received request %d messages from %s with wait of %d ms after every %d messages" % (message_count, client_host, wait_millis, block_size))
		else :
			print("Received request %d messages from %s (unthrottled)" % (message_count, client_host))
		data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		client_data_addr = (client_host, data_port)
		print("Sending %d messages to %s at port %d" % (message_count, client_host, data_port))
		i = 0
		ts1 = time.time()
		while i < message_count :
			data_sock.sendto(str(i).rjust(10), client_data_addr)
			i += 1
			if block_size and wait_millis :
				if not i % block_size : time.sleep(wait_millis / 1000.)
		ts2 = time.time()			
		ctl_sock.send(messages_sent_tmpl % message_count)
		print("==> %d messages sent in %f seconds" % (message_count, ts2 - ts1))

def receiver(sock, hostname, received, done) :
	sock.setblocking(0)
	waittime = 0
	recv_time = None
	done_time = None
	recv_count = 0
	while True :
		if done[0] and done_time is None : done_time = time.time()
		time.sleep(waittime)
		now = time.time()
		if done_time :
			if recv_time :
				if now - recv_time > .25 : break
			else :
				if now - done_time >  .5 : break
		try : 
			(msg, addr) = sock.recvfrom(4096)
			waittime = 0
		except socket.error as e :
			if e.args[0] == errno.EWOULDBLOCK :
				waittime = .05
				continue
			raise
		if addr[0] == hostname : 
			recv_time = time.time()
			recv_count  += 1
			received[int(msg.strip())] = True
	
def client(hostname, port, count, block_size, wait_millis) :
	hostname = socket.gethostbyname(hostname)
	if block_size and wait_millis :
		print("Requesting %d messages from %s with wait of %d ms after every %d messages" % (count, hostname, wait_millis, block_size))	
	else :
		print("Requesting %d messages from %s (unthrottled)" % (count, hostname))	
	data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	data_sock.bind((socket.gethostname(), 0))
	addr, data_port = data_sock.getsockname()
	received = count * [False]
	done = [False]
	t = threading.Thread(target=receiver, args=(data_sock, hostname, received, done))
	t.start()
	ctl_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	ctl_sock.connect((hostname, port))
	ctl_sock.send(message_request_tmpl % (count, data_port, block_size, wait_millis))
	msg = ctl_sock.recv(4096)
	assert msg.startswith(messages_sent)
	done[0] = True
	t.join()
	data_sock.close()
	sent = int(msg.split(":")[1])
	assert sent == count
	dropped_count = count - len([_f for _f in received if _f])
	percent_dropped = 100. * dropped_count / count
	print("%d of %d (%.1f%%) messages dropped" % (dropped_count, count, percent_dropped))
	ctl_sock.shutdown(socket.SHUT_RDWR)
	ctl_sock.close()
	
def shutdown(hostname, port) :
	ctl_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	ctl_sock.connect((hostname, port))
	ctl_sock.send(request_shutdown)
	ctl_sock.shutdown(socket.SHUT_RDWR)
	ctl_sock.close()

do_client   = False
do_server   = False
do_shutdn   = False
hostname    = None
port        = None
count       = None
block_size  = 0
wait_millis = 0

i = 1
while i < len(sys.argv) :
	if sys.argv[i] == "-client" :
		do_client = True
	elif sys.argv[i] == "-server" :
		do_server = True
	elif sys.argv[i] == "-shutdown" :
		do_shutdn = True
	elif sys.argv[i] == "-h" :
		hostname = sys.argv[i+1]
		i += 1
	elif sys.argv[i] == "-p" :
		port = int(sys.argv[i+1])
		i += 1
	elif sys.argv[i] == "-c" :
		count = int(sys.argv[i+1])
		i += 1
	elif sys.argv[i] == "-b" :
		block_size = int(sys.argv[i+1])
		i += 1
	elif sys.argv[i] == "-w" :
		wait_millis = int(sys.argv[i+1])
		i += 1
	i += 1 

if do_client and hostname and port and count and not (do_server or do_shutdn) :
	client(hostname, port, count, block_size, wait_millis)
elif do_server and port and not (do_client or do_shutdn or hostname or count) :
	server(port)
elif do_shutdn and hostname and port and not (do_client or do_server or count) :
	shutdown(hostname, port)
else :
	print('''
Argments must be "-client -h <host> -p <port> -c <msg_count> [-b <block_size> -w <wait_ms>]" 
	      or "-server -p <port>"
	      or "-shutdown -h <host> -p <port>"
	      
If <block_size> and <wait_ms> are specified, the server will pause <wait_ms> milliseconds
after sending every <block_size> messages. If they are not specified, the server will
send all <msg_count> message unthrottled
	''')
	

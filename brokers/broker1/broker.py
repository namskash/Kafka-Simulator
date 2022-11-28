import socket
import threading
from time import time
from datetime import date
import subprocess


# Connection Data
host = '127.0.0.1'
port = 55555		# port of broker

# Starting Server
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((host, port))
server.listen()

# Lists For Clients and Their topics
producers = {}
consumers = {}

# Sending Messages To All Connected Clients
def broadcast(message,topic,counter):
	print(message)

	_date = str(date.today())
	_time = str(time())
	message = message + "," + _date + "," + _time

	key = topic.split("topic(")[-1].split(')')[0]		# TO get 'BD' from 'topic(BD)'

# For all the consumers listening right now, just send the message
	if key in consumers:
		for client in consumers[key]:
			ack = None
			while ack != '1':
				client.send(message.encode('ascii'))
				ack = client.recv(10).decode('ascii')

# Write to partitions as well
	o = subprocess.run(["mkdir", "-p",topic])		#,capture_output=True,text=True)

	f0 = open('{}/p{}_c0.txt'.format(topic, counter%3), 'a')
	f0.write(message + "\n")
	f0.close()
	
	f1 = open('{}/p{}_c1.txt'.format(topic, counter%3), 'a')    
	f1.write(message + "\n")
	f1.close()

	f2 = open('{}/p{}_c2.txt'.format(topic, counter%3), 'a')
	f2.write(message + "\n")
	f2.close()

def broadcastFromBeg(client,topic):
	try:
		f0 = open('{}/p0_c0.txt'.format(topic), 'r')
		f1 = open('{}/p1_c0.txt'.format(topic), 'r')
		f2 = open('{}/p2_c0.txt'.format(topic), 'r')
		
		for line in f0:
			line = line.strip()
			ack = None
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

		for line in f1:
			line = line.strip()
			ack = None
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

		for line in f2:
			line = line.strip()
			ack = None
			while ack == None:
				client.send(line.encode('ascii'))
				ack = client.recv(10).decode('ascii')

	except:
		pass


# Handling Messages From Clients
def handle(client,address,topic,type):
	counter = 0
	topicCopy = topic
	topic = 'topic(' + topic + ')'
	if type == 'consumer+':
		broadcastFromBeg(client,topic)


	while True:
		try:
			# Broadcasting Messages
			message = None
			while message == None:
				message = client.recv(1024).decode('ascii')
		
			
			if message != "EXIT":
				#% send ACK
				client.send('1'.encode('ascii'))

				if message != '1':
					msg = message.split(':')
					broadcast(msg[1].strip(),topic,counter)
					counter += 1
			else:
				print("%s at port number: %d left"%(type,address[1]))
				client.close()
				if type == 'producer':
					producers[topicCopy].remove(client)
					print(producers)

				break	# exit this thread of handle
		except:
			# print("except")
			# Removing And Closing Clients
			client.close()
			print("%s at port number: %d left"%(type,address[1]))

			if type == 'producer':
				producers[topicCopy].remove(client)
			elif 'consumer' in type:
				consumers[topicCopy].remove(client)
			
			break


# Receiving / Listening Function
def receive():
	while True:
		# Accept Connection
		client, address = server.accept()
		print("Connected! Port number: {}".format(address[1]))

		# Request And Store topic
		topic = None
		while topic == None:
			client.send('TOPIC'.encode('ascii'))
			topic = client.recv(1024).decode('ascii')

		#% send ACK
		client.send(str(address[1]).encode('ascii'))

		#sleep(1)
		type = None
		while type == None:
			client.send('TYPE'.encode('ascii'))
			type = client.recv(1024).decode('ascii')

		#% send ACK
		client.send('1'.encode('ascii'))

		if type == 'producer':
			# Keep a collection of clients grouped by topic
			if topic in producers:
				producers[topic].append(client)
			else:
				producers[topic] = [client]

		elif 'consumer' in type:
			# Keep a collection of clients grouped by topic
			if topic in consumers:
				consumers[topic].append(client)
			else:
				consumers[topic] = [client]

		else: 	#zookeeper
			pass


		# Print And Broadcast topic
		print("Topic: {}, type: {}".format(topic,type))
		ack = None
		while ack == None:
			client.send('Connected to server!'.encode('ascii'))
			ack = client.recv(10)

		# Start Handling Thread For Client
		thread = threading.Thread(target=handle, args=(client,address,topic,type))
		thread.start()


print('broker is running')
receive()
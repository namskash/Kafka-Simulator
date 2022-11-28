import socket
import threading
from time import sleep
import sys

# Choosing topic
topic = input("Enter your topic: ")
type = 'producer'

broker_port = 55555

# Connecting To Server
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('127.0.0.1', broker_port))

# Listening to Server and Sending topic
def receive():
	while True:
		try:
			# Receive Message From Server
			# If 'TOPIC' Send topic
			message = client.recv(1024).decode('ascii')
			#sleep(5)
			if message == 'TOPIC':
				id = None
				#// While ack doesn't come
				while id == None:
					client.send(topic.encode('ascii'))
					id = client.recv(1024).decode('ascii')
				print("ProducerID received: ",id)

			elif message == 'TYPE':
				ack = None
				while ack == None:
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')
				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# message recieved ack
				if message!= '1':
					print(message)
		except:
			# Close Connection When Error
			print("The broker closed the connection! Try again after 30 seconds...")
			client.close()
			break

# Sending Messages To Broker
def write():
	while True:
		message = 'topic({}): {}'.format(topic, input('type your msg: '))
		#"""
		if "EXIT" in message:
			client.send("EXIT".encode('ascii'))
			print("exiting...")
			sleep(5)
			client.close()
			#exit()
			break
		#"""
		#? Send
		else:
			ack = None
			while ack == None:
				client.send(message.encode('ascii'))
				ack = client.recv(10).decode('ascii')


# Starting Threads For Listening And Writing
receive_thread = threading.Thread(target=receive)
receive_thread.start()

sleep(3)
write_thread = threading.Thread(target=write)
write_thread.start()


#zookeeper

zoo_port = 33333

# Connecting To Server
zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zoo.connect(('127.0.0.1', zoo_port))

# Listening to Server and Sending topic
def receive_zoo():
	while True:
		try:
			# Receive Message From Server
			# If 'TOPIC' Send topic
			message = zoo.recv(1024).decode('ascii')
			#sleep(5)
			if message == 'TOPIC':
				id = None
				#// While ack doesn't come
				while id == None:
					zoo.send(topic.encode('ascii'))
					id = zoo.recv(1024).decode('ascii')
				print("ProducerID received: ",id)

			elif message == 'TYPE':
				ack = None
				while ack == None:
					zoo.send(type.encode('ascii'))
					ack = zoo.recv(1024).decode('ascii')
				print("ACK recieved for type")

			else:
				zoo.send('1'.encode('ascii'))	# message recieved ack
				if message!= '1':
					print(message)
		except:
			# Close Connection When Error
			print("The broker closed the connection! Try again after 30 seconds...")
			zoo.close()
			break

# Sending Messages To Broker
def write_zoo():
	while True:
		message = 'topic({}): {}'.format(topic, input('type your msg: '))
		#"""
		if "EXIT" in message:
			zoo.send("EXIT".encode('ascii'))
			print("exiting...")
			sleep(5)
			zoo.close()
			#exit()
			break
		#"""
		#? Send
		else:
			ack = None
			while ack == None:
				zoo.send(message.encode('ascii'))
				ack = zoo.recv(10).decode('ascii')


# Starting Threads For Listening And Writing
receive_zoo_thread = threading.Thread(target=receive_zoo)
receive_zoo_thread.start()

sleep(3)
write_zoo_thread = threading.Thread(target=write_zoo)
write_zoo_thread.start()

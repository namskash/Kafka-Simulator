import socket
import threading
from time import sleep
import sys

if len(sys.argv) > 3:
	exit()

# Choosing topic
topic = input("Choose your topic: ")
type = 'consumer'

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

			if message == 'TOPIC':
				id = None
				#// While ack doesn't come
				while id == None:
					client.send(topic.encode('ascii'))
					id = client.recv(1024).decode('ascii')
				print("ConsumerID received:",id)

			elif message == 'TYPE':
				ack = None
				while ack != '1':
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')
				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# message recieved ack
				if message!= '1':
					print(message)
		except:
			# Close Connection When Error
			print("An error occured! Retrying...")
			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			client.connect(('127.0.0.1', broker_port))
			#client.close()
			#break

# Sending Messages To Server
"""
def write():
	while True:
		message = '{}: {}'.format(topic, input('type your msg:'))
		client.send(message.encode('ascii'))
"""


# Starting Threads For Listening And Writing
receive_thread = threading.Thread(target=receive)
receive_thread.start()

# write_thread = threading.Thread(target=write)
# write_thread.start()
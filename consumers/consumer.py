import socket
import threading
from time import sleep
import sys


# Choosing topic
topic = input("Choose your topic: ")
type = 'consumer'

if '--from-beginning' in sys.argv:
	# print(sys.argv)
	type += '+'
	
broker_port = 55555


# Connecting To Broker
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(('127.0.0.1', broker_port))

Exit = False

# Listening to Server and Sending topic
def receive():
	global client
	while True:
		try:
			message = client.recv(1024).decode('ascii')

			if message == 'TOPIC':
				id = None
				#// If ack doesn't come keep sending topic
				while id == None:
					client.send(topic.encode('ascii'))
					id = client.recv(1024).decode('ascii')

				print("ConsumerID received:",id)

			elif message == 'TYPE':
				ack = None
				#// If ack doesn't come keep sending topic
				while ack != '1':
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')

				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# message recieved ack

				if message!= '1':
					print(message)

		except:
			
			# print("exception: ",e)
			
			sleep(2)
			
			if Exit:
				break

			print("Connection with broker failed!\nRetrying after 15 seconds...")
			sleep(15)

			connected = False
			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

			while connected == False:
				try:
					client.connect(('127.0.0.1',broker_port))
					connected = True

				except:
					client.close()
					break
				
			sleep(1)


# Starting Threads For Listening
receive_thread = threading.Thread(target=receive)
receive_thread.start()

import socket
import threading
from time import sleep

# Choosing topic
topic = input("Enter your topic: ")
type = 'producer'

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

				print("ProducerID received:",id)

			elif message == 'TYPE':
				ack = None
				#// If ack doesn't come keep sending topic
				while ack == None:
					client.send(type.encode('ascii'))
					ack = client.recv(1024).decode('ascii')

				print("ACK recieved for type")

			else:
				client.send('1'.encode('ascii'))	# message recieved ack
				
				if message!= '1':
					print(message)

		except Exception as e:

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

					write_thread = threading.Thread(target=write)
					write_thread.start()

				except:
					break

			sleep(1)

# Sending Messages To Broker

def write():
	while True:
		message = 'topic({}): {}'.format(topic, input())

		if "EXIT" in message:
			client.send("EXIT".encode('ascii'))
			print("exiting...")
			sleep(1)
			client.close()
			global Exit
			Exit = True
			break
		
		# Send
		else:
			ack = None
			while ack == None:
				client.send(message.encode('ascii'))
				ack = client.recv(10).decode('ascii')


# Starting thread for listening
receive_thread = threading.Thread(target=receive)
receive_thread.start()

# Starting thread for writing
write_thread = threading.Thread(target=write)
write_thread.start()
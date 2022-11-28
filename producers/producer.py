import socket
import threading
from time import sleep

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

			if message == 'TOPIC':
				id = None
				#// While ack doesn't come
				while id == None:
					client.send(topic.encode('ascii'))
					id = client.recv(1024).decode('ascii')
				print("ProducerID received:",id)

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
			print("The broker closed the connection!")
			client.close()

			client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			client2.connect(('127.0.0.1',55556))
			
			#break

# Sending Messages To Broker
def write():
	while True:
		message = 'topic({}): {}'.format(topic, input('type your msg: '))
		#"""
		if "EXIT" in message:
			client.send("EXIT".encode('ascii'))
			print("exiting...")
			sleep(1)
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

sleep(2)
write_thread = threading.Thread(target=write)
write_thread.start()
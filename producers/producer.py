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

# Listening to Server and Sending topic
def receive():
	global client
	while True:
		try:
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
		except Exception as e:
			print("exception: ",e)
			# Close Connection When Error
			print("Failed! Retrying after 30 seconds",end='')
			for i in range(15):
				sleep(1)
				print('.',end='')
			print()
			#client.close()

			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			connected = False
			while connected == False:
				try:
					client.connect(('127.0.0.1',broker_port))
					sleep(2)
					print("type your msg: ")
					connected = True

					write_thread = threading.Thread(target=write)
					write_thread.start()
				except:
					#print("Failed. Retrying...")
					#sleep(2)
					client.close()
					break
			sleep(1)
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

sleep(3)
write_thread = threading.Thread(target=write)
write_thread.start()
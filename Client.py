import socket
import ipaddress
import threading
import time
import contextlib
import errno

maxPacketSize = 1024
defaultPort = 4000 # TODO: Change this to your expected port
serverIP = '127.0.0.1' #TODO: Change this to your instance IP

tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
try:
    tcpPort = int(input("Please enter the TCP port of the host..."));
except:
    tcpPort = 0;
if tcpPort == 0:
    tcpPort = defaultPort;
tcpSocket.connect((serverIP, tcpPort));

clientMessage = "";
while clientMessage != "exit":
    clientMessage = input("Please type the message that you'd like to send (Or type 'exit' to exit):\n>")

    # Send the message to the server
    tcpSocket.sendall(clientMessage.encode())

    # Wait for a reply from the server
    serverMessage = tcpSocket.recv(maxPacketSize).decode('utf-8')
    # Print the best highway to take
    print(f"Recommended highway: {serverMessage}")
    
tcpSocket.close();


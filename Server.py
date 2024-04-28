import socket
import ipaddress
import threading
import time
import contextlib
import errno
from dataclasses import dataclass
import random
import sys

maxPacketSize = 1024
defaultPort = 4000 #TODO: Set this to your preferred port

def GetFreePort(minPort: int = 1024, maxPort: int = 65535):
    for i in range(minPort, maxPort):
        print("Testing port",i);
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as potentialPort:
            try:
                potentialPort.bind(('localhost', i));
                potentialPort.close();
                print("Server listening on port",i);
                return i
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    print("Port",i,"already in use. Checking next...");
                else:
                    print("An exotic error occurred:",e);

def GetServerData() -> []:
    import MongoDBConnection as mongo
    return mongo.QueryDatabase();






def ListenOnTCP(tcpSocket: socket.socket, socketAddress):
    try:
        while True:
            clientMessage = tcpSocket.recv(maxPacketSize)
            if not clientMessage:
                print(f"Client at {socketAddress} has disconnected.")
                break
            
            clientMessage = clientMessage.decode('utf-8')
            print(f"Client message from {socketAddress}:", clientMessage)
            
            # TODO: Implement the logic to process the client message and query the database if needed
            response = clientMessage.upper()
            tcpSocket.send(response.encode('utf-8'))
            
            print("MOngoDB BS")
            returnedData = GetServerData()
            print("Returning Data")
            print(returnedData)
            
    except ConnectionResetError:
        print(f"Connection reset by {socketAddress}.")
    except Exception as e:
        print(f"An error occurred with the client at {socketAddress}: {e}")
    finally:
        tcpSocket.close()






def CreateTCPSocket() -> socket.socket:
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
    tcpPort = defaultPort
    print("TCP Port:",tcpPort);
    tcpSocket.bind(('localhost', tcpPort));
    return tcpSocket;

def LaunchTCPThreads():
    tcpSocket = CreateTCPSocket();
    tcpSocket.listen(5);
    while True:
        connectionSocket, connectionAddress = tcpSocket.accept();
        connectionThread = threading.Thread(target=ListenOnTCP, args=[connectionSocket, connectionAddress]);
        connectionThread.start();

if __name__ == "__main__":
    tcpThread = threading.Thread(target=LaunchTCPThreads);
    tcpThread.start();
    exitSignal = False

    while not exitSignal:
        time.sleep(1);
    print("Ending program by exit signal...");

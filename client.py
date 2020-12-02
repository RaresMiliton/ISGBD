import socket
from prettytable import PrettyTable

msg = ""

server = ("127.0.0.1", 9999)
bufferSize = 1024

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print("HEY! WHAT IS YOUR COMMAND?")

while msg != "exit":
    msg = input(">>> ")
    clientSocket.sendto(msg.encode(), server)
    server_response = clientSocket.recvfrom(bufferSize)
    if server_response[0].decode() != "SELECT":
        print(server_response[0].decode())
    else:
        f = open("databases/select.txt", "r")
        print(f.read())
        f.close()
import socket

msg = ""

server = ("127.0.0.1", 9999)
bufferSize = 1024

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print("HEY! WHAT IS YOUR COMMAND?")

while msg != "exit":
    msg = input(">>> ")
    clientSocket.sendto(msg.encode(), server)
    server_response = clientSocket.recvfrom(bufferSize)
    print(server_response[0].decode())
import socket
import random
import json


class Mensagem:
    def __init__(self, request, key, value, timestamp):
        self.request = request
        self.key = key
        self.value = value
        self.timestamp = timestamp

        # TIMESTAMP EH POR key, ENTAO INCREMENTA A CADA PUT


class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.sList = []

    def INIT(self):
        for i in range(1, 4):
            sIP = input(f"Server #{i} IP: ")
            sPort = input(f"Server #{i} Port: ")
            self.sList.append((sIP, int(sPort)))

    def PUT(self, key, value):
        try:
            serverSelect = random.randint(0, 2)
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect(
                (self.sList[serverSelect][0], self.sList[serverSelect][1]))

            msg = json.dumps(Mensagem("PUT", key, value, "0").__dict__)
            self.client.send(msg.encode())
            response = json.loads(self.client.recv(1024).decode())

            verify = response["request"]
            print(
                f"{verify} key: [{response['key']}]"
                "value: [{response['value']}]"
                "timestamp: [{response['timestamp']}]"
                "realizada no servidor [{self.sList[serverSelect][0]}:{self.sList[serverSelect][1]}]")

            self.client.shutdown(socket.SHUT_RDWR)
            self.client.close()

        except socket.error as e:
            print(f"error in PUT request {e}")

    def GET(self, key):
        try:
            serverSelect = random.randint(0, 2)
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect(
                (self.sList[serverSelect][0], self.sList[serverSelect][1]))

            msg = json.dumps(Mensagem("GET", key, "NULL", "0").__dict__)
            self.client.send(msg.encode())
            response = json.loads(self.client.recv(1024).decode())

            if response["request"] == "GET_ERROR":
                print("TRY_OTHER_SERVER_OR_LATER")
            else:
                print(f"GET key: [{key}]"
                      "value: [{response['value']}]"
                      "obtido do servidor [{self.sList[serverSelect][0]}:{self.sList[serverSelect][1]}],"
                      "meu timestamp [{response['timestamp']}] e do servidor [{response['timestamp']}]")

            self.client.shutdown(socket.SHUT_RDWR)
            self.client.close()

        except socket.error as e:  # Tratamento de erros
            print(f"error in GET request {e}")


def main():
    h = input("Digite o IP: ")
    p = input("Digite a Porta: ")

    client = Client(h, p)
    while True:
        # Client options
        print("\n")
        print("Select an option:")
        print("0 - Client initialization")
        print("1 - PUT request")
        print("2 - GET request")
        print("3 - Stop")
        print("\n")

        escolha = input("Option: ")

        if escolha == "0":
            print("Starting initialization ...")
            client.INIT()

        elif escolha == "1":
            print("Starting PUT request...")
            if client.sList == []:
                print("Please, initialize the client first")
            else:
                key = input("Insert the KEY: ")
                value = input("Insert the VALUE: ")
                client.SOCKET_CONNECTION()
                client.PUT(key, value)

        elif escolha == "2":
            print("Starting GET request...")
            if client.sList == []:
                print("Please, initialize the client first")
            else:
                key = input("Insert the KEY: ")
                client.SOCKET_CONNECTION()
                client.GET(key)

        elif escolha == "3":
            print("Stopping process ...")
            break

        else:
            print("PLease, select an option from 0 to 3.")


main()

import socket
import random
import json

# Criação da classe de mensagem, pela qual são enviadas as informacoes aos servidores
class Message:
    def __init__(self, request, key, value, timestamp):
        self.request = request
        self.key = key
        self.value = value
        self.timestamp = timestamp


class Client:
    def __init__(self):
        self.sList = []
        self.timestamp = 0

    # Inicialização do cliente, obtendo o ip e porta de cada um dos servidores e armazenando na lista sList
    def INIT(self):
        for i in range(1, 4):
            sIP = input(f"Server #{i} IP: ")
            sPort = input(f"Server #{i} Port: ")
            self.sList.append((sIP, int(sPort)))

    # Envio do PUT, iniciando na seleção de um servidor aleatório da lista sList
    def PUT(self, key, value):
        try:
            serverSelect = random.randint(0, 2)
            # Envio da requisição
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect(
                (self.sList[serverSelect][0], self.sList[serverSelect][1]))

            # Recepção da resposta do servidor
            tempMsg = Message("PUT", key, value, "0")
            msg = json.dumps(tempMsg.__dict__)
            self.client.send(msg.encode())
            response = json.loads(self.client.recv(1024).decode())

            # Tratamento da resposta
            # "verify" é utilizado para apresentar a resposta "PUT_OK" ou "PUT_ERROR"
            verify = response["request"]
            print(
                f"{verify} key: [{response['key']}] value: [{response['value']}] timestamp: [{response['timestamp']}] realizada no servidor [{self.sList[serverSelect][0]}:{self.sList[serverSelect][1]}]")

            self.timestamp = + 1
            self.client.close()

        except socket.error as e:
            print(f"error in PUT request {e}")

    # Envio do GET, iniciando na seleção de um servidor aleatório da lista sList
    def GET(self, key):
        try:
            rand = random.randint(0, 2)
            serverSelect = self.sList[rand]
            # Envio da requisição
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect((serverSelect[0], serverSelect[1]))

            # Recepção da resposta do servidor
            msg = json.dumps(Message("GET", key, "NULL", "0").__dict__)
            self.client.send(msg.encode())
            response = json.loads(self.client.recv(1024).decode())

            # Tratamento da resposta
            if response["request"] == "GET_OK":
                print(f"GET key: [{key}] value: [{response['value']}] obtido do servidor [{serverSelect[0]}:{serverSelect[1]}], meu timestamp [{response['timestamp']}] e do servidor [{response['timestamp']}]")
            else:
                print("TRY_OTHER_SERVER_OR_LATER")

            self.client.close()

        except socket.error as e: 
            print(f"error in GET request {e}")


def main():

    print("Starting initialization ...")
    client = Client()
    client.INIT()
    while True:
        # Client options
        print("\n")
        print("Select an option:")
        print("1 - PUT request")
        print("2 - GET request")
        print("3 - Stop")
        print("\n")

        escolha = input("Option: ")

        # Escolha do PUT, onde é feita a captura do teclado da chave e valor
        if escolha == "1":
            print("Starting PUT request...")
            key = input("Insert the KEY: ")
            value = input("Insert the VALUE: ")
            client.PUT(key, value)

        # Escolha do GET, onde é feita a captura do teclado da chave a ser buscada
        elif escolha == "2":
            print("Starting GET request...")
            key = input("Insert the KEY: ")
            client.GET(key)

        elif escolha == "3":
            print("Stopping process ...")
            break

        else:
            print("Please, select an option from 1 to 3.")


main()

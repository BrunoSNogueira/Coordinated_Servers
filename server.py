import socket
import threading
import json
import time
import random


class Message:
    def __init__(self, request, key, value, timestamp):
        self.request = request
        self.key = key
        self.value = value
        self.timestamp = timestamp

class Server:
    def __init__(self, localIP, localPort, leaderIP, leaderPort):
        self.localIP = localIP
        self.localPort = int(localPort)
        self.leaderIP = leaderIP
        self.leaderPort = int(leaderPort)
        self.dataBase = [["0", "Teste", "-1"]]
        self.serverList = [10097, 10098, 10099]
        self.serverList.remove(self.localPort)
        self.serverTS = 0

        # Define if this server is the Leader
        self.isLeader = self.leaderPort == self.localPort

        try:
            self.Server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.Server.bind((self.localIP, self.localPort))
        except socket.error as e:  # Tratamento de erros
            print(f"erro na criacao do socket erro {e}")

    # Uso de threads para responder simultaneamente os clientes
    def serverUp(self):
        self.Server.listen()  

        while True:
            connection, adrs = self.Server.accept()


            thread_processes = threading.Thread(target=self.multiThread, args=[connection, adrs])
            thread_processes.start()

    def multiThread(self, connection, adrs):
        req = connection.recv(1024).decode()
        message = json.loads(req)
        reqOption = message["request"]
        kv = f"key: [{message['key']}] value: [{message['value']}]"
        
        if reqOption == "PUT":
            if self.isLeader:
                print(f"Cliente [{adrs[0]}]:[{adrs[1]}] PUT {kv}")
            else:
                print(f"Encaminhando PUT {kv}")
                
            self.PUT(connection, adrs, req)

        elif reqOption == "GET":
            self.GET(connection, adrs, message)
            
        elif reqOption == "REPLICATION":
            print(
                f"REPLICATION {kv} ts: [{message['timestamp']}]")
            self.REPLICATION(connection, message)

        connection.shutdown(socket.SHUT_RDWR)
        connection.close()
        return

    # Requisições PUT, tratando o caso em que servidores que não são o lider que recebem a requisição
    def PUT(self, connection, adrs, req):
        if self.isLeader:
            message = json.loads(req)
            self.updateDB(message["key"], message["value"], self.serverTS)
            try:
                server1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                server2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                server1.connect(('127.0.0.1', self.serverList[0]))
                server2.connect(('127.0.0.1', self.serverList[1]))

                message_server_aux = Message(
                    "REPLICATION", message["key"], message["value"], self.serverTS)
                message_server = json.dumps(message_server_aux.__dict__)

                server1.send(message_server.encode())
                server2.send(message_server.encode())
                
                okFromServer1 = server1.recv(1024).decode()
                okFromServer2 = server2.recv(1024).decode()
                server1ToJson = json.loads(okFromServer1)
                server2ToJson = json.loads(okFromServer2)
                server1Request = server1ToJson["request"]
                server2Request = server2ToJson["request"]

                if server1Request != "REPLICATION_OK" or server2Request != "REPLICATION_OK":
                    messagePUT = Message(
                        "PUT_ERROR", message["key"], message["value"], message["timestamp"])
                else:
                    print(
                        f"Enviando PUT_OK ao Cliente {adrs[0]}:{adrs[1]} da key: [{message['key']}] ts: [{self.serverTS}]")
                    messagePUT = Message(
                        "PUT_OK", message["key"], message["value"], message["timestamp"])

                resp = json.dumps(messagePUT.__dict__)
                connection.send(resp.encode())

                server1.close()
                server2.close()

                # Controle do Timestamp
                self.serverTS = self.serverTS + 1

            except socket.error as e:
                print(f"error in PUT request {e}")

        else:
            LeaderServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            LeaderServer.connect((self.leaderIP, self.leaderPort))
            leaderRequest = req.encode()
            LeaderServer.send(leaderRequest)
            leaderReturn = LeaderServer.recv(1024).decode()
            connection.send(leaderReturn.encode())

    # Replication das informações para os outros servidores
    def REPLICATION(self, connectionServer, message):
        try:
            # Uso de ramdomizer para simular latencia
            latency = random.randint(1, 6)
            for i in range(0, latency):
                time.sleep(1)

            for item in self.dataBase:
                if item[0] == message["key"]:
                    item[1] = message["value"]
                    item[2] = message["timestamp"]

            messageRepl = Message(
                "REPLICATION_OK", message["key"], message["value"], message["timestamp"])
            resp = json.dumps(messageRepl.__dict__)

            connectionServer.send(resp.encode())

            self.serverTS = self.serverTS + 1
        except socket.error as e:
            print(f"error in Replicarion {e}")
    
    # Requisições GET
    def GET(self, connection, adrs, message):
        try:
            # Devolução Null da chave que não existe
            messageGET = Message("NULL", "NULL", "NULL", "0")
            
            for item in self.dataBase:
                if item[0] == message['key']:
                    # Comparacao do timestamp para obter o valor mais recente
                    if int(item[2]) < int(message["timestamp"]):
                        messageGET = Message(
                            "Error in GET request", item[0], "TRY_OTHER_SERVER_OR_LATER", item[2])
                    else:
                        messageGET = Message(
                            "GET_OK", item[0], item[1], item[2])

            print(f"Cliente {adrs[0]}:{adrs[1]}"
                f" GET key: [{message['key']}]"
                f" ts: [{message['timestamp']}]."
                f" Meu ts é [{messageGET.timestamp}], portanto devolvendo [{messageGET.value}]")
            
            response = json.dumps(messageGET.__dict__)
            connection.send(response.encode())
        except socket.error as e:
            print(f"error in GET request {e}")

def main():
    # Inicialização do servidor recebendo informações ,capturadas pelo teclado, locais e do lider
    ip = input("Insert local IP: ")
    port = input("Insert local Port: ")
    ipLeader = input("Insert leader IP: ")
    portLeader = input("Insert leader Port: ")

    server = Server(ip, port, ipLeader, portLeader)
    server.serverUp()

main()

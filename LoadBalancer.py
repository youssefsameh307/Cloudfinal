from aiohttp import web
import asyncio
import socket
import random
import os
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient


class LoadBalancer:

    def __init__ (self,load_balancer_ip,load_balancer_port,http_client_ip,http_client_port):
        self.load_balancer_port = load_balancer_port
        self.load_balancer_ip = load_balancer_ip
        self.cache_ports = dict([])
        self.cache_ips = dict([])
        self.http_client_ip = http_client_ip
        self.http_client_port= http_client_port
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=team43project;AccountKey=ECsl3Tug62RLOD9+RAm8Swzff4izvyLgdSEjBbsh/slgGe0cqhdmptUhClOtkrymvIrY/ZMZ48hu+AStpwNLzA==;EndpointSuffix=core.windows.net"
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client("cloud-project")

        # if True:
        #     for i in range(0,30):
        #         print('starting file upload: ', i)
        #         currentFileName ='clickbench_id' + str(i)+'.csv'
        #         CSVfilePath = os.path.join(os.getcwd(), currentFileName)

        #         with open(CSVfilePath, 'r', encoding='utf-8') as file:
        #             data = file.read()
                    
        #         self.container_client.upload_blob(name=currentFileName, data=data, overwrite=True)
        
    async def start(self):
        self.cache_server_task = asyncio.create_task(self.cache_server(self.load_balancer_port))
        self.http_client_task = asyncio.create_task(self.http_client())
        await asyncio.gather(self.cache_server_task, self.http_client_task)

    async def liste_for_cache_server(self,client_socket, answer):
        intermediate = client_socket.recv(1024)
        print("received from cache server: ", intermediate)
        answer['data'] = intermediate.decode('utf-8')

    async def send_to_cache_server(self,type,id, data=None, timeout=30):
        # Connect to the cache server via a socket and send the message
        message = type + '_' + id
        if type == 'set':
            message += '_' + data
        answer = dict()
        client_socket = None
        try:
            testI = int(id)
            target_port = self.get_next_target_port(id)
            if target_port is None:
                return 'None'
            target_ip = self.cache_ips[target_port][0]
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((str(target_ip), int(target_port)))  # Adjust the cache server's address and port
            client_socket.sendall(message.encode('utf-8'))
            print(f"Sent message {message} to the Cache Server")
            await asyncio.wait_for(self.liste_for_cache_server(client_socket,answer), timeout)
            return answer['data']
        except asyncio.TimeoutError:
            print(f"No response received within {timeout} seconds")
            self.cache_ports.pop(target_port, None)
            return None
        except ConnectionRefusedError:
            print("Connection refused")
            self.cache_ports.pop(target_port, None)  
            return None
        except Exception as ex:
            print('Exception:', ex)
            return None
        finally:
            if client_socket is not None:
                client_socket.close()

    def str_hash_function(self,id_str, num_partitions):
        # Calculate the sum of ASCII values of characters in the ID
        key = sum(ord(char) for char in id_str)

        if num_partitions==0:
            return None
        
        # Use modulo to determine the partition number
        partition_number = key % num_partitions
        
        return partition_number

    def hash_function(self,key, num_partitions):
        # Ensure key is non-negative
        key = abs(key)
        
        # Use modulo to determine the partition number
        partition_number = key % num_partitions
        
        return partition_number

    def get_next_target_port(self,id):
        pool = list(self.cache_ports.keys())

        if(pool == []):
            print("no cache server available")
            return None
        
        targetFileNumber = int(id) // 123333
        targetPageNumber = int(id) % 123333 // 1000
        cache_key = str(targetFileNumber) + "_" + str(targetPageNumber)

        num = self.str_hash_function(cache_key,len(pool))

        target = pool[num]
        print(f"chose target cache port based on random: {target}")
        return target

    async def handle_Cache_registeration(self,reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"Accepted connection from {addr}")

        try:
            while True:
                data = await reader.read(100)
                if not data:
                    break
                message = data.decode('utf-8')
                try:
                    intMessage = int(message)
                    self.cache_ports[message] = 0
                    self.cache_ips[message] = addr
                    print(self.cache_ports)
                    print(f"Received message from Cache Server: {message}")
                except:
                    print(f"Received abnormal registeration request: {message}")

        finally:
            print(f"Closing connection from {addr}")
            writer.close()


    def update_data_by_id(self, data, id, new_data):
        lines = data.split('\n')
        updated_lines = []
        for line in lines:
            parts = line.split('\t')
            if len(parts) == 2 and parts[0] == id:
                parts[1] = new_data
            updated_lines.append('\t'.join(parts))
        updated_data = '\n'.join(updated_lines)
        return updated_data
    

    async def handle_request(self,request):
        param = request.match_info['param']
        # Extract content from the HTTP request
        content = await request.text()
        targetFileNumber = int(param) // 123333
        targetFileName = 'clickbench_id' + str(targetFileNumber)+'.csv'
        try:
            # Download the blob to a stream
            data = self.container_client.get_blob_client(targetFileName).download_blob().readall()
            data = data.decode('utf-8')
            data = self.update_data_by_id(data, param, content)
            self.container_client.upload_blob(name=targetFileName, data=data,overwrite=True)
            print(f'changed data with id ${param} into ${content}')
            
        except Exception as ex:
            print('Exception:', ex)

        # Send the content to the cache server via a socket
        check = await self.send_to_cache_server('set', param, content) #id, data
        if check =="None":
            return web.Response(text="No cache server available")
        if check == None:
            return web.Response(text="No response received within 5 seconds")
        
        # return the result here or an error message 

        return web.Response(text=check)

    async def handle_get_request(self,request):
        # Extract the parameter from the HTTP request
        param = request.match_info['param']
        print(param)
        # Send the parameter to the cache server via a socket
        check = await self.send_to_cache_server("get", param)
        if check =="None":
            return web.Response(text="No cache server available")
        if check == None:
            return web.Response(text="No response received within 5 seconds")
        if len(check) < 2:
            return web.Response(text="abnormal request")
        print("check: ", check)
        return web.json_response({'hit':check[0],'data':check[1:]})

    async def cache_server(self,Listen_port):
        server = await asyncio.start_server(
            self.handle_Cache_registeration, self.load_balancer_ip, self.load_balancer_port)
        addr = server.sockets[0].getsockname()
        print(f"Cache Server listening on {addr}")

        async with server:
            await server.serve_forever()

    async def http_client(self):
        app = web.Application()
        app.router.add_post('/{param}', self.handle_request)
        app.router.add_get('/{param}', self.handle_get_request)
        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, self.http_client_ip, self.http_client_port)
        await site.start()

        print("HTTP Client listening on: ", str(self.http_client_ip)+":" +str(self.http_client_port))

        try:
            while True:
                await asyncio.sleep(3600)  # Run indefinitely
        finally:
            await runner.cleanup()



async def main():
    MyLoadBalancer = LoadBalancer('0.0.0.0',8082,'0.0.0.0',8080)
    await MyLoadBalancer.start()

if __name__ == "__main__":
    asyncio.run(main())

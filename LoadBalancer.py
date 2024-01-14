from aiohttp import web
import asyncio
import socket
import random


class LoadBalancer:

    def __init__ (self,load_balancer_ip,load_balancer_port,http_client_ip,http_client_port):
        self.load_balancer_port = load_balancer_port
        self.load_balancer_ip = load_balancer_ip
        self.cache_ports = dict([])
        self.cache_ips = dict([])
        self.http_client_ip = http_client_ip
        self.http_client_port= http_client_port
        
        
    async def start(self):
        self.cache_server_task = asyncio.create_task(self.cache_server(self.load_balancer_port))
        self.http_client_task = asyncio.create_task(self.http_client())
        await asyncio.gather(self.cache_server_task, self.http_client_task)

    async def liste_for_cache_server(self,client_socket, answer):
        intermediate = client_socket.recv(1024)
        print("received from cache server: ", intermediate)
        answer['data'] = intermediate.decode('utf-8')

    async def send_to_cache_server(self,type,id, data=None, timeout=5):
        # Connect to the cache server via a socket and send the message
        message = type + '_' + id
        if type == 'set':
            message += '_' + data
        answer = dict()
        client_socket = None
        try:
            target_port = self.get_next_target_port(id)
            if target_port is None:
                return 'None'
            target_ip = self.cache_ips[target_port][0]
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((str(target_ip), int(target_port)))  # Adjust the cache server's address and port
            client_socket.sendall(message.encode('utf-8'))
            self.cache_ports[target_port] += 1
            print(f"Sent message {message} to the Cache Server")
            await asyncio.wait_for(self.liste_for_cache_server(client_socket,answer), timeout)
            self.cache_ports[target_port] -= 1
            return answer['data']
        except asyncio.TimeoutError:
            print(f"No response received within {timeout} seconds")
            self.cache_ports.pop(target_port, None)
            return None
        except ConnectionRefusedError:
            print("Connection refused")
            self.cache_ports.pop(target_port, None)  
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
        
        num = self.str_hash_function(id,len(pool))

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
                self.cache_ports[message] = 0
                self.cache_ips[message] = addr
                print(self.cache_ports)
                print(f"Received message from Cache Server: {message}")
        finally:
            print(f"Closing connection from {addr}")
            writer.close()

    async def handle_request(self,request):
        param = request.match_info['param']
        # Extract content from the HTTP request
        content = await request.text()
        
        # Send the content to the cache server via a socket
        check = await self.send_to_cache_server('set', param, content)
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
        
        return web.Response(text=check)

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
    MyLoadBalancer = LoadBalancer('0.0.0.0',8082,'20.234.153.127',8080)
    await MyLoadBalancer.start()

if __name__ == "__main__":
    asyncio.run(main())

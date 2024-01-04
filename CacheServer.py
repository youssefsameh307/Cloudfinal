import asyncio
import os
import random
import socket
import sys

from enum import Enum
import time
import socket


class CacheCell:
    def __init__(self, data, meta_data):
        self.data = data
        self.meta_data = meta_data
    
    def __str__(self):
        return "Data: " + str(self.data) + "\nMeta Data: " + str(self.meta_data)
    
    def getData(self):
        return self.data
    
    def getMetaData(self,key):
        return self.meta_data[key]
    
    def setData(self,data):
        self.data = data

class onDiskCacheCell:
    def __init__(self, data, meta_data, path, key):
        self.path = path
        self.key = key
        self.data = os.path.join(self.path, self.key)
        self.meta_data = meta_data
        print('creating new cell with path: ', self.data)
        try:
            print('started')
            with open(self.data, 'w') as file:
                file.write(str(data))
                print('finished for path', self.data)
        except:
            print("error writing to file")
    
    def __str__(self):
        return "Data: " + str(self.data) + "\nMeta Data: " + str(self.meta_data) + "\nPath: " + str(self.path) + "\nKey: " + str(self.key)
    
    def getData(self):
        data=None
        try:
            with open(self.data, 'r') as file:
                data = file.read()
        except:
            print("error reading file")
            data = None
        return data
    
    def getMetaData(self,key):
        return self.meta_data[key]
    
    def setData(self, data):
        try:
            with open(self.data, 'w') as file:
                file.write(str(data))
        except:
            print("error writing to file")

class replacement_policy_enum(Enum):
    LRU = 1
    MRU = 2
    LFU = 3

class Cache:
    def __init__(self, maxSize, replacement_policy, patience,port,mode):
        self.maxSize = maxSize
        self.patience = patience
        self.replacement_policy = replacement_policy
        self.port = str(port)
        self.cache = dict()
        self.mode = mode
        if mode=='onDisk':
            self.path = os.path.join(os.getcwd(), self.port)
            if os.path.exists(self.path):
                for file_name in os.listdir(self.path):
                    file_path = os.path.join(self.path, file_name)
                    os.remove(file_path)
            else:
                os.mkdir(self.port)
    def __str__(self):
        return "Max Size: " + str(self.maxSize) + "\nReplacement Policy: " + str(self.replacement_policy) + "\nCache: " + str(self.cache)
    
    def get(self, key):
        print('getting key: ', key)
        myVal = key
        if key not in self.cache.keys():
            print(self.cache)
            print("not found, contacting database")
            rand = random.randint(0, 100) # get value from database
            print("got value from database ", rand)
            self.set(myVal, rand)
            print('passed await')
            return str(rand)
        self.cache[key].meta_data['last use'] = time.time()
        self.cache[key].meta_data['use count'] += 1
        return str(self.cache[key].getData())
    
    def execute_cache_policy(self):
        if self.replacement_policy == replacement_policy_enum.LRU:
            # Find the least recently used item
            least_recently_used = None
            for key in self.cache.keys():
                if least_recently_used == None:
                    least_recently_used = key
                else:
                    if self.cache[key].getMetaData('last use') < self.cache[least_recently_used].getMetaData('last use'):
                        least_recently_used = key
            # Delete the least recently used item
            if self.mode =='onDisk':
                try:
                    filePath = self.cache[least_recently_used].data
                    print(self.cache[least_recently_used])
                    os.remove(filePath)
                    print("cache File deleted successfully.")
                except OSError as e:
                    print(f"Error deleting the file: {e}")
            del self.cache[least_recently_used]
        elif self.replacement_policy == replacement_policy_enum.MRU:
            # Find the most recently used item
            most_recently_used = None
            for key in self.cache.keys():
                if most_recently_used == None:
                    most_recently_used = key
                else:
                    if self.cache[key].getMetaData('last use') > self.cache[most_recently_used].getMetaData('last use'):
                        most_recently_used = key
            # Delete the most recently used item
            if self.mode =='onDisk':
                try:
                    filePath = self.cache[least_recently_used].data
                    os.remove(filePath)
                    print("cache File deleted successfully.")
                except OSError as e:
                    print(f"Error deleting the file: {e}")
            del self.cache[most_recently_used]
        elif self.replacement_policy == replacement_policy_enum.LFU:
            # Find the least frequently used item
            least_frequently_used = None
            for key in self.cache.keys():
                if least_frequently_used == None:
                    least_frequently_used = key
                else:
                    if self.cache[key].getMetaData('use count') < self.cache[least_frequently_used].getMetaData('use count'):
                        least_frequently_used = key
            # Delete the least frequently used item
            if self.mode =='onDisk':
                try:
                    filePath = self.cache[least_recently_used].data
                    os.remove(filePath)
                    print("cache File deleted successfully.")
                except OSError as e:
                    print(f"Error deleting the file: {e}")
            del self.cache[least_frequently_used]
        else:
            raise Exception("Invalid replacement policy")

    def set(self, key, data):
        print('setting key: ', key)
        myKey = key
        if len(self.cache) >= self.maxSize:
            self.execute_cache_policy()
            
        print('creating new cache cell for ', myKey)
        print(self.mode)

        meta_data= dict()
        meta_data['last use'] = time.time()
        meta_data['use count'] = 0
        meta_data['patience'] = self.patience
        if self.mode == 'onDisk':
            print('creating onDiskCacheCell for key: ', myKey)
            self.cache[myKey] = onDiskCacheCell(data, meta_data, self.path, myKey)
        else:
            self.cache[myKey] = CacheCell(data, meta_data)


async def handle_loadbalancer_request(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Accepted connection from {addr}")

    try:
        while True:
            data = await reader.read(100)
            if not data:
                break

            message = data.decode('utf-8')
            method = message.split('_')[0]
            key = message.split('_')[1]
            response = ''
            if method == 'get':
                response = myCache.get(key)
            else:
                data = message.split('_')[2]
                myCache.set(key, data)
                response = 'ACK'
            # fill with cache logic

            writer.write(response.encode('utf-8'))
            print(f"Received message from HTTP Client: {message}")
    finally:
        print(f"Closing connection from {addr}")
        writer.close()

async def connect_to_load_balancer(cache_port):
    # Connect to the load balancer
    load_balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    load_balancer_socket.connect(('localhost', 8082))  # Adjust the load balancer's address and port
    load_balancer_socket.sendall(str(cache_port).encode('utf-8'))
    print(f"Sent cache_port {cache_port} to the Load Balancer")

    # Perform any necessary communication with the load balancer
    # ...

    # Close the connection to the load balancer
    load_balancer_socket.close()
    print("Connection to the Load Balancer closed")

async def main(mode,cache_port):
    global myCache
    myCache = Cache(5, replacement_policy_enum.LRU, 2,cache_port, mode)
    await connect_to_load_balancer(cache_port)
    print('cache_port ', cache_port)
    server = await asyncio.start_server(
        handle_loadbalancer_request, '0.0.0.0', cache_port)  # Adjust the port as needed

    addr = server.sockets[0].getsockname()
    print(f"Cache Server listening on {addr}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <cache_port>")
        sys.exit(1)

    cache_port = int(sys.argv[2])
    mode = str(sys.argv[1])
    asyncio.run(main(mode, cache_port))

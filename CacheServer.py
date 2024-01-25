import asyncio
import os
import random
import socket
import sys
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from enum import Enum
import time
import socket
import math


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
    MRU = 3
    LFU = 2

class Cache:
    def __init__(self, maxSize, replacement_policy, patience,port,mode):
        self.maxSize = maxSize
        self.patience = patience
        self.replacement_policy = replacement_policy
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=team43project;AccountKey=ECsl3Tug62RLOD9+RAm8Swzff4izvyLgdSEjBbsh/slgGe0cqhdmptUhClOtkrymvIrY/ZMZ48hu+AStpwNLzA==;EndpointSuffix=core.windows.net"
        
        # self.blob_client = BlobClient.from_connection_string(self.connection_string, 'cloud-project', 'cache_database') #container name , blob name

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.blob_client = self.container_client = self.blob_service_client.get_container_client("cloud-project")

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
    
    def read_blob(self,fileName):
        try:
            # Download the blob to a stream
            data = self.container_client.get_blob_client(fileName).download_blob().readall()
            # data = self.blob_client.download_blob().readall()
            return data
        except Exception as ex:
            print('Exception:', ex)


    def contact_db(self,key):
        # Save the file to the container ---- get pages, a page contains 1000 records so a file contains 124 pages
        targetFileNumber = int(key) // 123333
        targetFileName = 'clickbench_id' + str(targetFileNumber)+'.csv'
        

        targetPageNumber = int(key) % 123333 // 1000
        start = targetPageNumber * 1000
        end = min(123333,start + 1000)


        file_content = self.read_blob(targetFileName)
        file_lines = file_content.decode('utf-8').split('\n')
        page = dict()

        # for i in range(len(file_lines)):
        #     line = file_lines[i]
        #     if line.startswith(key+'\t'):
        #         content = line.split('\t')[1]
        #         for j in range(max(0,i+1-additionalRecords), min(len(file_lines),i+additionalRecords+1)):
        #             nameList = file_lines[j].split('\t')

        #             if len(nameList) < 2:
        #                 return None
                    
        #             additionalList[nameList[0]] = nameList[1]
        #         return content , additionalList
        
        print("searching for: ",start)
        for i in range(len(file_lines)):
            line = file_lines[i]
            if i == start:
                print('found start')
                print(line)
                for j in range(i,end):
                    nameList = file_lines[j].split('\t')
                    if len(nameList) < 2:
                        return None
                    page[nameList[0]] = nameList[1]
                break
        print('not found start')
        if page.keys() == []:
            return None
        else:
            return page
        # return None

    def get(self, key):
        print('getting key: ', key)
        targetFileNumber = int(key) // 123333
        targetPageNumber = (int(key) % 123333) // 1000
        cache_key = str(targetFileNumber) + "_" + str(targetPageNumber)
        if cache_key not in self.cache.keys():
            # print(self.cache)
            # print("not found, contacting database")

            data = self.contact_db(key) # get value from database
            if data == None or data[key] == None:
                print("key not found in database")
                return None
            
            print("got value from database ", data[key])
            self.set(key, data, True)

            print('passed await')
            return str(data[key]), 'F'
        self.cache[cache_key].meta_data['last use'] = time.time()
        self.cache[cache_key].meta_data['use count'] += 1
        return self.cache[cache_key].data[key], 'T'
    
    def execute_cache_policy(self):
        if self.replacement_policy == 'lru':
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
        elif self.replacement_policy == 'mru':
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
        elif self.replacement_policy == 'lfu':
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

    def set(self, key, data,getFromDB=False):
        print('setting key: ', key)
        myKey = key

        targetFileNumber = int(myKey) // 123333
        targetPageNumber = int(myKey) % 123333 // 1000
        cache_key = str(targetFileNumber) + "_" + str(targetPageNumber)      

        if len(self.cache) >= self.maxSize:
            self.execute_cache_policy()
            
        # print('creating new cache cell for ', myKey)
        # print(self.mode)

        meta_data1= dict()
        meta_data1['last use'] = time.time()
        meta_data1['use count'] = 0
        meta_data1['patience'] = self.patience
        if self.mode == 'onDisk':
            # print('creating onDiskCacheCell for key: ', myKey)
            self.cache[myKey] = onDiskCacheCell(data, meta_data1, self.path, myKey)
        else:
            # oldCell = self.cache[cache_key]
            # oldMetaData = self.cache[cache_key].meta_data
            oldUseCount = 0
            cacheData = dict()


            if cache_key in self.cache.keys():
                oldUseCount = self.cache[cache_key].meta_data['use count']
                cacheData = self.cache[cache_key].data

            if getFromDB:
                cacheData = data
            else:
                cacheData[myKey] = data

            meta=dict()
            meta['last use'] = time.time()
            meta['use count'] = oldUseCount
            meta['patience'] = self.patience
            self.cache[cache_key] = CacheCell(cacheData, meta)


async def handle_loadbalancer_request(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Accepted connection from {addr}")
    message= None
    try:
        while True:
            data = ''
            while True:

                chunk = await reader.read(100)
                if not chunk:
                    break
                data += chunk
            
            if not data:
                break

            message = data.decode('utf-8')
            print("received message from HTTP Client: ", message)
            method = message.split('_')[0]
            key = message.split('_')[1]
            response = ''
            found = None
            if method == 'get':
                response, found = myCache.get(key)
            else:
                data = message.split('_')[2:]
                if len(data) == 1:
                    data = data[0]
                else:
                    data = '_'.join(data)
                myCache.set(key, data)
                response = 'ACK'
            # fill with cache logic
            if response ==None:
                response = 'Not found'
            if found != None:
                response = found + response
            writer.write(response.encode('utf-8'))
            print(f"Finished with HTTP message : {message}")
    except Exception as e:
        print("exceptionL ", e)
        print("abnormal message ", message)
        print("received abnormal request")

    finally:
        print(f"Closing connection from {addr}")
        writer.close()

async def connect_to_load_balancer(loadbalancer_ip,cache_port):
    # Connect to the load balancer
    load_balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    load_balancer_socket.connect((loadbalancer_ip, 8082))  # Adjust the load balancer's address and port
    load_balancer_socket.sendall(str(cache_port).encode('utf-8'))
    print(f"Sent cache_port {cache_port} to the Load Balancer")
    load_balancer_socket.close()
    print("Connection to the Load Balancer closed")

async def main(mode,cache_port,replacement_policy):
    global myCache
    # myCache = Cache(5, replacement_policy_enum.LRU, 2,cache_port, mode)
    myCache = Cache(40, replacement_policy, 0,cache_port, mode)
    lbip = '20.113.69.217'
    if mode == 'onmem':
        lbip = 'localhost' 
    await connect_to_load_balancer(lbip,cache_port)
    print('cache_port ', cache_port)
    server = await asyncio.start_server(
        handle_loadbalancer_request, '0.0.0.0', cache_port)  # Adjust the port as needed

    addr = server.sockets[0].getsockname()
    print(f"Cache Server listening on {addr}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script_name.py <cache_port>")
        sys.exit(1)

    rePo = ['lfu','mru','lru']
    if sys.argv[3] not in rePo:
        print("Usage: python script_name.py <cache_port> <replacement_policy>")
        sys.exit(1)
    replacement_policy = str(sys.argv[3])
    # print(replacement_policy_enum.LRU)
    cache_port = int(sys.argv[2])
    mode = str(sys.argv[1])
    asyncio.run(main(mode, cache_port, replacement_policy))

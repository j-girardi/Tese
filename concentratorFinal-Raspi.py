import socket
import struct
import fcntl
import time
import pymongo
import threading



def multicast_config():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    INTERFACE = socket.inet_ntoa(fcntl.ioctl(s.fileno(),0x8915, struct.pack('256s', ifname[:15]))[20:24]) # IP on the ethernet interface
    global sock
    ifname = 'eth0'
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    INTERFACE = socket.inet_ntoa(fcntl.ioctl(sock.fileno(), 0x8915, struct.pack('256s', bytes(ifname[:15], 'utf-8')))[20:24])
    GROUP_IP = "224.3.29.71"
    GROUP_PORT = 10000
    global multicast_group
    multicast_group = (GROUP_IP, GROUP_PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    ip_pack = struct.pack("4s4s", socket.inet_aton(GROUP_IP), socket.inet_aton(INTERFACE))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, ip_pack)
    sock.settimeout(1)
    print(INTERFACE)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216) #buffer size increase

    

def mongo_config():
    client = pymongo.MongoClient(
        "mongodb://a39622:Bf34VVGBVLpF6Qy6@on-surf-shard-00-00-obkot.mongodb.net:27017,"
        "on-surf-shard-00-01-obkot.mongodb.net:27017,"
        "on-surf-shard-00-02-obkot.mongodb.net:27017/test?ssl=true&replicaSet=On-Surf-shard-0&authSource=admin"
        "&retryWrites=true&w=majority")
    db = client.esp_data
    global posts
    posts = db.data


def data_split():
    if address[0] not in dic_backup:
        dic_backup[address[0]] = data

    if data != dic_backup[address[0]]:
        sensor1, sensor2, sensor3, sensor4, sensor5, sensor6, sensor7, sensor8, timestamp, count_id = data.decode().split('$$')

        post_1 = {
            'Timestamp': timestamp,
            'Module Address': address,
            'Data counter': count_id,
            'Sensor 1': sensor1,
            'Sensor 2': sensor2,
            'Sensor 3': sensor3,
            'Sensor 4': sensor4,
            'Sensor 5': sensor5,
            'Sensor 6': sensor6,
            'Sensor 7': sensor7,
            'Sensor 8': sensor8
        }
        global dataList
        global c
        dataList.append(post_1)
        if (c%2000== 0):
            t1 = time.time()
            posts.insert_many(dataList)
            t2 = time.time()
            dataList = []
            print('Data posted! ..... time taken = {} s'.format(t2-t1))
        dic_backup[address] = data



dataList = []
dic_backup = {}
multicast_config()
mongo_config()
c = 0
try:

    while True:
        # Send data
        message = str(time.time())
        sent = sock.sendto(message.encode(), multicast_group)  # timestamp with millisecond resolution
        
        print('waiting to receive')
        while True:
            try:
                c = c + 1
                data, address = sock.recvfrom(256)
            except socket.timeout:
                print('timed out, no more responses.........................')
                print('')
                if c % 5 == 0:
                    break
            else:
                print('%d received "%s" from %s' % (c, data.decode(), address))
                data_split()
                if c % 100000 == 0:
                    break
finally:
    print("closing socket")
    sock.close()


                                        
                                        

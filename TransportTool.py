# -*- coding:utf-8 -*-
"""
Dec: 文件传送工具
Created on: 2019.10.14
Author: Iflier
Modified on: 2019.10.17
对 PUSH/PULL 连接绑定对，添加暂存正在使用的fundcode
Modified on: 2019.10.18
添加统计发送文件的速率
"""
import os
import re
import sys
import json
import time
import os.path
import socket
import struct
import string
import pickle
import threading
from datetime import datetime

import zmq

from ..GeneralCrawlerTool.Base import SingletonRedis


class BaseTransfer(object):
    """公共基类
    """
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port if isinstance(port, str) else str(port)
        self.cache = SingletonRedis.getRedisInstance()
        self.filepathTemp = string.Template(os.path.join(os.getcwd(), "Fund", "dedupApiGot", "${filename}"))

class Transfer(BaseTransfer):
    
    BufferSize = 4096
    StructFormat = "9s I"
    
    def __init__(self, ip, port):
        super(Transfer, self).__init__(ip, port)
        self.fileInfoStruct = struct.Struct(self.StructFormat)
    
    def sendFile(self):
        print("[INFO] Sending ...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(150)
        sock.bind((self.ip, self.port))
        print("[INFO] Waiting for connection ...")
        sock.listen()
        conn, addr = sock.accept()
        print("[INFO] A connection received from: {0}".format(addr))
        
        if "inuseTransferFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransferFundCode"):
                self.cache.smove("inuseTransferFundCode", "transferFundCode", mem)
        
        stratTime = time.time()
        totalNum = self.cache.scard("transferFundCode")
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transferFundCode", count=None)
            if fundCode is None:
                # 没有更多的元素了
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if not os.path.exists(filepath):
                # 如果指定的文件不存在，则什么也不做
                continue
            self.cache.sadd("inuseTransferFundCode", fundCode)
            fileHeader = self.fileInfoStruct.pack(fundCode, os.path.getsize(filepath))
            conn.send(fileHeader)
            with open(filepath, 'rb') as file:
                while True:
                    data = file.read(self.BufferSize)
                    if not data:
                        break
                    conn.send(data)
            self.cache.srem("inuseTransferFundCode", fundCode)
        conn.send(self.fileInfoStruct.pack("exit", 0))
        conn.close()
        sock.close()
        print("[INFO] Transfer rate: {0} / s".format(round(totalNum / (time.time() - stratTime), 2)))
    
    def recvFile(self):
        print("[INFO] Start at {0}".format(datetime.now().strftime("%c")))
        print("[INFO] Receiving ...")
        sock = socket.create_connection((self.ip, self.port), 150)
        while True:
            fileHeader = sock.recv(struct.calcsize(self.StructFormat))
            filename, fileSize = self.fileInfoStruct.unpack(fileHeader)
            filenameStripped = filename.strip(b'\x00').decode().lower()
            if filenameStripped in ["exit", "quit"]:
                break
            restSize = fileSize
            filepath = self.filepathTemp.substitute(filename=".".join([filenameStripped, "json"]))
            with open(filepath, 'wb') as file:
                while restSize > 0:
                    if restSize >= self.BufferSize:
                        data = sock.recv(self.BufferSize)
                    else:
                        data = sock.recv(restSize)
                    file.write(data)
                    restSize -= self.BufferSize

        sock.close()
        print("[INFO] Complete at {0}".format(datetime.now().strftime("%c")))


class TransferWithZMQ(BaseTransfer):
    """发送一个文件需要对端来回通信 2 次
    """
    def __init__(self, ip, port):
        super(TransferWithZMQ, self).__init__(ip, port)
        self.ctx = zmq.Context.instance()
    
    def sendFile(self):
        sock = self.ctx.socket(zmq.REQ)
        sock.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transportFundCode", count=None)
            if fundCode is None:
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if not os.path.exists(filepath):
                continue
            fileContent = None
            sock.send_string("next,{0}".format(fundCode))  # 第一次发送：发送下一个文件的名称
            self.cache.sadd("inuseTransferFundCode", fundCode)
            if sock.recv_string().lower() == "ok":
                with open(filepath, 'r', encoding="utf-8") as file:
                    fileContent = file.read()
                sock.send_pyobj(pickle.dumps(fileContent))  # 第二次发送：发送关于下一个文件的内容
                sock.recv_string()
                self.cache.srem("inuseTransferFundCode", fundCode)
            else:
                break
        sock.send_string("exit,0")  # 通知对端退出
        sock.close()
        self.ctx.destroy()
        
    
    def recvFile(self):
        sock = self.ctx.socket(zmq.REP)
        sock.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        while True:
            infoStr = sock.recv_string().lower()
            info, fundCode = infoStr.split(',')
            if info == "next":
                sock.send_string("ok")
                filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
                pickledContent = sock.recv_pyobj()
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(pickle.loads(pickledContent))
                sock.send_string("ok")
            else:
                break
        sock.close()
        self.ctx.destroy()

class TransferWithZMQPP(BaseTransfer):
    """消息大小一般为几个KB到几个MB
    """
    def __init__(self, ip, port):
        super(TransferWithZMQPP, self).__init__(ip, port)
        self.ctx = zmq.Context.instance()
    
    def sendFile(self, peerNumber=1):
        sock = self.ctx.socket(zmq.PUSH)
        sock.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        # 如果没有一次性传送完所有的文件，下次启动时接着传
        if "inuseTransferFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransferFundCode"):
                self.cache.smove("inuseTransferFundCode", "transferFundCode", mem)
        
        startTime = time.time()
        filesNum = self.cache.scard("transferFundCode")
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transferFundCode")
            if fundCode is None:
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if not os.path.exists(filepath):
                continue
            fileContent = None
            self.cache.sadd("inuseTransferFundCode", fundCode)
            with open(filepath, 'r', encoding="utf-8") as file:
                fileContent = file.read()
            sock.send_string(json.dumps(dict(fundcode=fundCode, content=fileContent), ensure_ascii=False))
            self.cache.srem("inuseTransferFundCode", fundCode)
        for _ in range(peerNumber):
            sock.send_string(json.dumps(dict(fundcode="exit", content=""), ensure_ascii=False))
        sock.close()
        self.ctx.destroy()
        endTime = time.time()
        print("[INFO] Totally, use {0:^9.2f} seconds, average transfer speed: {1} files / minute".format(endTime - startTime, 0 if filesNum == 0 else round(filesNum / ((endTime - startTime) / 60), 0)))
    
    def recvFile(self):
        sock = self.ctx.socket(zmq.PULL)
        sock.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        while True:
            msg = json.loads(sock.recv_string(), encoding="utf-8")
            if msg["fundcode"].lower() in ["exit", "quit"]:
                print("[INFO] Exit ...")
                break
            else:
                filepath = self.filepathTemp.substitute(filename=".".join([msg["fundcode"], "json"]))
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(msg["content"])
        sock.close()
    
    def recvFilesWithMultiThreads(self):
        thList = list()
        for _ in range(10):
            thList.append(threading.Thread(target=self.recvFile, args=()))
        for th in thList:
            th.start()
        for th in thList:
            th.join()
        self.ctx.term()


class TransferWithZMQREPDEALER(BaseTransfer):
    def __init__(self, ip, port, peerNumber):
        super(TransferWithZMQREPDEALER, self).__init__(ip, port)
        self.peerNumber = peerNumber
        self.ctx = zmq.Context.instance()
    
    def sendFile(self):
        sock = self.ctx.socket(zmq.DEALER)
        sock.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        # 如果没有一次性传送完所有的文件，下次启动时接着传
        if "inuseTransferFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransferFundCode"):
                self.cache.smove("inuseTransferFundCode", "transferFundCode", mem)
        
        startTime = time.time()
        filesNum = self.cache.scard("transferFundCode")
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transferFundCode")
            if fundCode is None:
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if not os.path.exists(filepath):
                continue
            fileContent = None
            self.cache.sadd("inuseTransferFundCode", fundCode)
            with open(filepath, 'r', encoding='utf-8') as file:
                fileContent = file.read()
            sock.send_multipart([fundCode.encode(encoding='utf-8'), b'', fundCode.encode(encoding='utf-8'), fileContent.encode(encoding='utf-8')])
            finishedFundCode, _, status = list(map(lambda x: x.decode(), sock.recv_multipart()))
            if status.lower() in ["ok",]:
                if re.search(r"^\d+", finishedFundCode):
                    self.cache.srem("inuseTransferFundCode", finishedFundCode)
        for _ in range(self.peerNumber):
            sock.send_multipart([b'0', b'', b'exit', b''])
        sock.close()
        self.ctx.term()
        endTime = time.time()
        print("[INFO] Totally, use {0:^9.2f} seconds, average transfer speed: {1} files / minute".format(endTime - startTime, 0 if filesNum == 0 else round(filesNum / ((endTime - startTime) / 60), 0)))
    
    def recvFile(self):
        sock = self.ctx.socket(zmq.REP)
        sock.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        while True:
            fundCode, fileContent = list(map(lambda x: x.decode(), sock.recv_multipart()))
            if fundCode.lower() in ["exit", "quit"]:
                print("[INFO] Exit ...")
                break
            else:
                filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(fileContent)
                sock.send(b'ok')
        sock.close()
    
    def recvFilesWithMultiThreads(self):
        thList = list()
        for _ in range(self.peerNumber):
            thList.append(threading.Thread(target=self.recvFile, args=()))
        for th in thList:
            th.start()
        for th in thList:
            th.join()
        self.ctx.term()


class TransferWithZMQREQROUTER(BaseTransfer):
    def __init__(self, ip, port, peerNumber):
        super(TransferWithZMQREQROUTER, self).__init__(ip, port)
        self.peerNumber = peerNumber
        self.ctx = zmq.Context.instance()
    
    def sendFile(self):
        # 如果没有一次性传送完所有的文件，下次启动时接着传
        if "inuseTransferFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransferFundCode"):
                self.cache.smove("inuseTransferFundCode", "transferFundCode", mem)
        startTime = time.time()
        filesNum = self.cache.scard("transferFundCode")
        sender = self.ctx.socket(zmq.ROUTER)
        signalPull = self.ctx.socket(zmq.PULL)
        sender.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        # 用于接收对端退出的通知
        signalPull.bind("tcp://{0}".format(":".join([self.ip, str(int(self.port) + 1)])))
        poller = zmq.Poller()
        poller.register(sender, zmq.POLLIN)
        poller.register(signalPull, zmq.POLLIN)
        while True:
            try:
                socks = dict(poller.poll(timeout=150 * 1000))
            except KeyboardInterrupt as err:
                print("[ERROR] {0}".format(err))
                break
            if sender in socks:
                address, _, fundCode = sender.recv_multipart()
                filepath = self.filepathTemp.substitute(filename=".".join([fundCode.decode(), "json"]))
                if not os.path.exists(filepath):
                    sender.send_multipart([address, b'', b'next', b''])
                    continue
                fileContent = None
                with open(filepath, 'r', encoding="utf-8") as file:
                    fileContent = file.read()
                sender.send_multipart([address, b'', b'ok', fileContent.encode()])
            if signalPull in socks:
                if signalPull.recv_string().lower() in ["exit",]:
                    self.peerNumber -= 1
                    if not self.peerNumber:
                        break
        sender.close()
        signalPull.close()
        self.ctx.term()
        endTime = time.time()
        print("[INFO] Totally, use {0:^9.2f} seconds, average transfer speed: {1} files / minute".format(endTime - startTime, 0 if filesNum == 0 else round(filesNum / ((endTime - startTime) / 60), 0)))
    
    def recvFile(self):
        receiver = self.ctx.socket(zmq.REQ)
        signalPush = self.ctx.socket(zmq.PUSH)
        receiver.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        signalPush.connect("tcp://{0}".format(":".join([self.ip, str(int(self.port) + 1)])))
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transferFundCode", count=None)
            if fundCode is None:
                continue
            receiver.send_string(fundCode)  # 请求对端发送这个代码的文件内容
            status, content = list(map(lambda x: x.decode(), receiver.recv_multipart()))
            if status in ["next",]:
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if status in ["ok",]:
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(content)
        signalPush.send_string("exit")  # 通知对端可以退出了
        receiver.close()
        signalPush.close()
    
    def recvFilesWithMultiThreads(self):
        thList = list()
        for _ in range(self.peerNumber):
            thList.append(threading.Thread(target=self.recvFile, args=()))
        for th in thList:
            th.start()
        for th in thList:
            th.join()
        self.ctx.term()


class TransferWithZMQREQROUTERSimplify(BaseTransfer):
    """类似于上面的类，但是没有专门用于接收对端通知的 socket 类型。对端的退出，通过判断分隔符之后的第一个帧的内容来决定
    """
    def __init__(self, ip, port, peerNumber):
        super(TransferWithZMQREQROUTER, self).__init__(ip, port)
        self.peerNumber = peerNumber
        self.ctx = zmq.Context.instance()
    
    def sendFile(self):
        sock = self.ctx.socket(zmq.ROUTER)
        sock.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        while True:
            address, _, fundCode = sock.recv_multipart()
            if re.search(r"^\d+", fundCode.decode()):
                filepath = self.filepathTemp.substitute(filename=".".join([fundCode.decode(), "json"]))
                if not os.path.exists(filepath):
                    sock.send_multipart([address, b'', b'next', b''])
                else:
                    fileContent = None
                    with open(filepath, 'r', encoding="utf-8") as file:
                        fileContent = file.read()
                    sock.send_multipart([address, b'', b'ok', fileContent.encode()])
            elif re.search(r"^\w+", fundCode.decode()):
                if fundCode.decode().lower() in ["exit",]:
                    self.peerNumber -= 1
                    if not self.peerNumber:
                        break
            else:
                print("[ERROR] Sender received an unexpected message: {0}".format(fundCode.decode()))
                break
        sock.close()
        self.ctx.term()
    
    def recvFile(self):
        sock = self.ctx.socket(zmq.REQ)
        sock.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        while "transferFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transferFundCode", count=None)
            if fundCode is None:
                continue
            sock.send_string(fundCode)  # 请求对端发送这个代码的文件内容
            status, content = list(map(lambda x: x.decode(), sock.recv_multipart()))
            if status in ["next",]:
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if status in ["ok",]:
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(content)
        sock.send_string("exit")  # 通知对端退出的消息
        sock.close()
    
    def recvFilesWithMultiThreads(self):
        thList = list()
        for _ in range(self.peerNumber):
            thList.append(threading.Thread(target=self.recvFile, args=()))
        for th in thList:
            th.start()
        for th in thList:
            th.join()
        self.ctx.term()

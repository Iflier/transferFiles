# -*- coding:utf-8 -*-
"""
Dec: 文件传送工具
Created on: 2019.10.14
Author: Iflier
Modified on: 2019.10.17
对 PUSH/PULL 连接绑定对，添加暂存正在使用的fundcode
"""
import os
import sys
import json
import time
import os.path
import socket
import struct
import string
import pickle
from datetime import datetime

import zmq

from FundTools.GeneralCrawlerTool.Base import SingletonRedis


class BaseTransfer(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.cache = SingletonRedis.getRedisInstance()
        self.filepathTemp = self.filepathTemp = string.Template(os.path.join(os.getcwd(), "Fund", "apiGot", "${filename}"))

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
    """发送一个文件需要对端来回通信 2 次。也不便于开多线程
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
    
    def sendFile(self):
        sock = self.ctx.socket(zmq.PUSH)
        sock.bind("tcp://{0}".format(":".join([self.ip, self.port])))
        # 如果没有一次性传送完所有的文件，下次启动时接着传
        if "inuseTransferFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransferFundCode"):
                self.cache.smove("inuseTransferFundCode", "transferFundCode", mem)
        
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
        sock.send_string(json.dumps(dict(fundcode="exit", content=""), ensure_ascii=False))
        sock.close()
        self.ctx.destroy()
    
    def recvFile(self):
        sock = self.ctx.socket(zmq.PULL)
        sock.connect("tcp://{0}".format(":".join([self.ip, self.port])))
        while True:
            msg = json.loads(sock.recv_string(), encoding="utf-8")
            if msg["fundcode"].lower() in ["exit", "quit"]:
                break
            else:
                filepath = self.filepathTemp.substitute(filename=".".join([msg["fundcode"], "json"]))
                with open(filepath, 'w', encoding='utf-8') as file:
                    file.write(msg["content"])
        sock.close()
        self.ctx.destroy()

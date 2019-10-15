# -*- coding:utf-8 -*-
"""
Dec: 文件传送工具
Created on: 2019.10.14
Author: Iflier
"""
import os
import sys
import json
import time
import os.path
import socket
import struct
import string
from datetime import datetime

from FundTools.GeneralCrawlerTool.Base import SingletonRedis


class Transport(object):
    
    BufferSize = 4096
    StructFormat = "9s I"
    
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.cache = SingletonRedis.getRedisInstance()
        self.filepathTemp = string.Template(os.path.join(os.getcwd(), "Fund", "apiGot", "${filename}"))
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
        
        if "inuseTransportFundCode" in self.cache.keys():
            for mem in self.cache.smembers("inuseTransportFundCode"):
                self.cache.smove("inuseTransportFundCode", "transportFundCode", mem)
        
        stratTime = time.time()
        totalNum = self.cache.scard("transportFundCode")
        while "transportFundCode" in self.cache.keys():
            fundCode = self.cache.spop("transportFundCode", count=None)
            if fundCode is None:
                # 没有更多的元素了
                continue
            filepath = self.filepathTemp.substitute(filename=".".join([fundCode, "json"]))
            if not os.path.exists(filepath):
                # 如果指定的文件不存在，则什么也不做
                continue
            self.cache.sadd("inuseTransportFundCode", fundCode)
            fileHeader = self.fileInfoStruct.pack(fundCode, os.path.getsize(filepath))
            conn.send(fileHeader)
            with open(filepath, 'rb') as file:
                while True:
                    data = file.read(self.BufferSize)
                    if not data:
                        break
                    conn.send(data)
            self.cache.srem("inuseTransportFundCode", fundCode)
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

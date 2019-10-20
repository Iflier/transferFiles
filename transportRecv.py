# -*- coding:utf-8 -*-
"""
Dec: 接收文件
Created on: 2019.10.15
Author: Iflier
"""
from .TransportTool import TransferWithZMQPP


host, ip = "192.168.0.3", 50000
trans = TransferWithZMQPP(host, ip)
trans.recvFilesWithMultiThreads()

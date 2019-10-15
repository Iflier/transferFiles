# -*- coding:utf-8 -*-
"""
Dec: 发送文件
Created on: 2019.10.15
Author: Iflier
"""
from .TransportTool import Transport


host, ip = "192.168.0.3", 50000
trans = Transport(host, ip)
trans.sendFile()

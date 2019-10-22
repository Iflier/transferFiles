# 一个用于局域网内两个机器之间传递大量小文件的工具
尽管`Windows`系统的远程桌面连接功能支持机器之间的复制，但是在传送大量小文件的时候，还是比较慢的。因此，打算用`Python`写个小工具来比较一下。所有的代码还没有测试，这个得等到下一轮我更新文件的时候再测吧!
## 注意
该工具使用`socket`连接在机器之间复制文件，还有更先进的基于`Python`语言的第三方库可以替代它。
## Update on 2019.10.16
使用先进的消息队列`ZMQ`在机器之间复制文件  
所有的代码还不曾测试过 !

## Update on 2019.10.20
下面是局域网内的测试片段，最慢8分多钟传完。嗯，的确比远程桌面连接下复制文件快得多。  
`PULL-PUSH` 模式，使用起来很方便，但是`PUSH` socket类型端，无法从对端获取消息是否接收成功的确认
![image](https://github.com/Iflier/transferFiles/blob/master/fast.PNG)</br>

## Update on 2019.10.22
添加`DEALER-REP` socket 类型，`REP` socket类型在向对端回复消息的时候会带有地址。基本上可以用来解决`PULL-PUSH`模式无法获取确认的问题了
CPython version: `3.7.2`; pyzmq version: `18.1.0`
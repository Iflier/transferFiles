# 一个用于局域网内两个机器之间传递大量小文件的工具
尽管`Windows`系统的远程桌面连接功能支持机器之间的复制，但是在传送大量小文件的时候，还是比较慢的。因此，打算用`Python`写个小工具来比较一下。所有的代码还没有测试，这个得等到下一轮我更新文件的时候再测吧!
## 注意
该工具使用`socket`连接在机器之间复制文件，还有更先进的基于`Python`语言的第三方库可以替代它。
## Update on 2019.10.16
使用先进的消息队列`ZMQ`在机器之间复制文件  
所有的代码还不曾测试过 !

## Update on 2019.10.20
下面是局域网内的测试片段，最慢 `8` 分多钟传完。嗯，的确比远程桌面连接下复制文件快得多。  
`PULL-PUSH` 模式，使用起来很方便，但是`PUSH` socket类型端，无法从对端获取消息是否接收成功的确认</br>
![image](https://github.com/Iflier/transferFiles/blob/master/fast.PNG)</br>

## Update on 2019.10.22
添加`DEALER-REP` socket 类型，`REP` socket类型在向对端回复消息的时候会带有地址。基本上可以用来解决`PULL-PUSH`模式无法获取确认的问题了  
简单地尝试了一下`REQ-ROUTER` socket，要求最开始要由 `REQ` 对端发起。而 `REQ` 对端事先并不知道请求的文件是否存在，这就需要`REQ`在接受到消息后，对消息进行判断了  
## Update on 2019.10.25
前一个版本，文件发送方无法知道何时退出。而最佳退出时机，由文件接收方通知最为合适。因此，添加了 `PULL-PUSH` socket 类型，用于发送/接收退出的通知  
## Update on 2019.10.26
1. 更正类型转换时的一个错误。  
## Update on 2019.11.02
测试 `REP-DEALER` socket 对儿：</br>
![image](https://github.com/Iflier/transferFiles/blob/master/1.PNG)  
## Update on 2019.11.26
1. 更正一个继承了父类的子类的写法错误  
2. 添加了`DEALER`和`ROUTER` socket 对儿的用法</br>
CPython version: `3.7.2`; pyzmq version: `18.1.0`
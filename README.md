# C++ Reactor TCP Server

从零实现的 Reactor 模型 TCP 网络库，基于 epoll + 非阻塞 IO + 线程池。

## 项目结构

```
├── server.hpp          ← 核心单头文件库（Buffer/Socket/Channel/Poller/
│                          EventLoop/TimerWheel/Connection/TcpServer）
├── source/
│   ├── echo/           ← Echo 回显服务器示例
│   │   ├── echo.hpp
│   │   ├── main.cc
│   │   └── Makefile
│   └── http/           ← HTTP/1.1 服务器（支持静态资源 + RESTful 路由）
│       ├── http.hpp
│       ├── main.cc
│       ├── Makefile
│       └── wwwroot/
├── example/            ← 组件示例（Any、Bind、Eventfd、Socket、TimerFd、TimeWheel）
├── test/               ← 多客户端并发测试（1~6 客户端 + 服务器）
└── image/              ← 设计图
```

## 编译

```bash
# Echo 服务器
cd source/echo && make && ./main

# HTTP 服务器
cd source/http && make && ./main

# 测试
cd test && make && ./server
```

## 架构

```
┌─ TcpServer ──────────────────────────────────────┐
│  ┌─ EventLoop(main) ───────────────────────────┐ │
│  │  Acceptor (监听端口 → accept)               │ │
│  │  TimerWheel (非活跃连接超时销毁)             │ │
│  └──────────────────────────────────────────────┘ │
│  ┌─ LoopThreadPool ────────────────────────────┐ │
│  │  EventLoop(sub1)   EventLoop(sub2)  ...     │ │
│  │  Connection        Connection               │ │
│  └──────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

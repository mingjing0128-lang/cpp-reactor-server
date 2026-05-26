#ifndef __M_SERVER_H__
#define __M_SERVER_H__
#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <cstring>
#include <ctime>
#include <cerrno>
#include <functional>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <typeinfo>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

/* ================================================================
 * 日志系统
 * ================================================================ */
#define INF 0
#define DBG 1
#define ERR 2
#define LOG_LEVEL DBG

#define LOG(level, format, ...) do {\
    if (level < LOG_LEVEL) break;\
    time_t t = time(NULL);\
    struct tm *ltm = localtime(&t);\
    char tmp[32] = {0};\
    strftime(tmp, 31, "%H:%M:%S", ltm);\
    fprintf(stdout, "[%lx][%s %s:%d] " format "\n",\
            (unsigned long)pthread_self(), tmp, __FILE__, __LINE__, ##__VA_ARGS__);\
} while (0)

#define INF_LOG(format, ...) LOG(INF, format, ##__VA_ARGS__)
#define DBG_LOG(format, ...) LOG(DBG, format, ##__VA_ARGS__)
#define ERR_LOG(format, ...) LOG(ERR, format, ##__VA_ARGS__)

/* ================================================================
 * Buffer —— 非连续空间的环形缓冲区
 * 使用 vector<char> 管理内存，_reader_idx 标识读取位置，
 * _writer_idx 标识写入位置，可读数据范围 [_reader_idx, _writer_idx)
 * ================================================================ */
#define BUFFER_DEFAULT_SIZE 1024

class Buffer {
public:
    Buffer()
        : _buffer(BUFFER_DEFAULT_SIZE)
        , _reader_idx(0)
        , _writer_idx(0)
    {}

    /* ---------- 基础指针 ---------- */
    char *Begin() { return &*_buffer.begin(); }

    // 当前写入起始地址
    char *WritePosition() { return Begin() + _writer_idx; }

    // 当前读取起始地址
    char *ReadPosition() { return Begin() + _reader_idx; }

    /* ---------- 空间计算 ---------- */
    // 写偏移之后的可写空间
    uint64_t TailIdleSize() { return _buffer.size() - _writer_idx; }

    // 读偏移之前的可回收空间
    uint64_t HeadIdleSize() { return _reader_idx; }

    // 可读数据大小 = 写偏移 - 读偏移
    uint64_t ReadAbleSize() { return _writer_idx - _reader_idx; }

    /* ---------- 偏移移动 ---------- */
    void MoveReadOffset(uint64_t len) {
        if (len == 0) return;
        assert(len <= ReadAbleSize());
        _reader_idx += len;
    }

    void MoveWriteOffset(uint64_t len) {
        assert(len <= TailIdleSize());
        _writer_idx += len;
    }

    /* ---------- 空间保证 ---------- */
    // 确保有 len 字节的可写空间（不够则移动数据或扩容）
    void EnsureWriteSpace(uint64_t len) {
        if (TailIdleSize() >= len) return;
        if (len <= TailIdleSize() + HeadIdleSize()) {
            // 整体空闲空间够，但尾部不够 → 把数据移到头部
            uint64_t rsz = ReadAbleSize();
            std::copy(ReadPosition(), ReadPosition() + rsz, Begin());
            _reader_idx = 0;
            _writer_idx = rsz;
        } else {
            // 整体不够 → 扩容
            _buffer.resize(_writer_idx + len);
        }
    }

    /* ---------- 写入 ---------- */
    void Write(const void *data, uint64_t len) {
        if (len == 0) return;
        EnsureWriteSpace(len);
        std::copy((const char*)data, (const char*)data + len, WritePosition());
    }

    void WriteAndPush(const void *data, uint64_t len) {
        Write(data, len);
        MoveWriteOffset(len);
    }

    void WriteString(const std::string &data) {
        Write(data.c_str(), data.size());
    }

    void WriteStringAndPush(const std::string &data) {
        WriteString(data);
        MoveWriteOffset(data.size());
    }

    void WriteBuffer(Buffer &data) {
        Write(data.ReadPosition(), data.ReadAbleSize());
    }

    void WriteBufferAndPush(Buffer &data) {
        WriteBuffer(data);
        MoveWriteOffset(data.ReadAbleSize());
    }

    /* ---------- 读取 ---------- */
    void Read(void *buf, uint64_t len) {
        assert(len <= ReadAbleSize());
        std::copy(ReadPosition(), ReadPosition() + len, (char*)buf);
    }

    void ReadAndPop(void *buf, uint64_t len) {
        Read(buf, len);
        MoveReadOffset(len);
    }

    std::string ReadAsString(uint64_t len) {
        assert(len <= ReadAbleSize());
        std::string str;
        str.resize(len);
        Read(&str[0], len);
        return str;
    }

    std::string ReadAsStringAndPop(uint64_t len) {
        std::string str = ReadAsString(len);
        MoveReadOffset(len);
        return str;
    }

    /* ---------- 行读取（HTTP 协议用） ---------- */
    char *FindCRLF() {
        return (char*)memchr(ReadPosition(), '\n', ReadAbleSize());
    }

    // 获取一行（包含末尾换行符）
    std::string GetLine() {
        char *pos = FindCRLF();
        if (pos == NULL) return "";
        return ReadAsString(pos - ReadPosition() + 1);
    }

    std::string GetLineAndPop() {
        std::string str = GetLine();
        MoveReadOffset(str.size());
        return str;
    }

    /* ---------- 清空 ---------- */
    void Clear() {
        _reader_idx = 0;
        _writer_idx = 0;
    }

private:
    std::vector<char> _buffer;
    uint64_t _reader_idx;    // 读偏移
    uint64_t _writer_idx;    // 写偏移
};


/* ================================================================
 * Socket —— POSIX 套接字 RAII 封装
 * ================================================================ */
#define MAX_LISTEN 1024

class Socket {
public:
    Socket() : _sockfd(-1) {}
    explicit Socket(int fd) : _sockfd(fd) {}
    ~Socket() { Close(); }

    int Fd() { return _sockfd; }

    /* ---------- 基础操作 ---------- */
    bool Create() {
        _sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (_sockfd < 0) { ERR_LOG("CREATE SOCKET FAILED!"); return false; }
        return true;
    }

    bool Bind(const std::string &ip, uint16_t port) {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(ip.c_str());
        if (bind(_sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            ERR_LOG("BIND ADDRESS FAILED!"); return false;
        }
        return true;
    }

    bool Listen(int backlog = MAX_LISTEN) {
        if (listen(_sockfd, backlog) < 0) {
            ERR_LOG("LISTEN FAILED!"); return false;
        }
        return true;
    }

    bool Connect(const std::string &ip, uint16_t port) {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(ip.c_str());
        if (connect(_sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            ERR_LOG("CONNECT FAILED!"); return false;
        }
        return true;
    }

    int Accept() {
        int newfd = accept(_sockfd, NULL, NULL);
        if (newfd < 0) { ERR_LOG("ACCEPT FAILED!"); return -1; }
        return newfd;
    }

    /* ---------- 收发 ---------- */
    // recv 返回值：>0=数据长度, 0=EAGAIN/EINTR(无数据), -1=连接关闭或错误
    ssize_t Recv(void *buf, size_t len, int flag = 0) {
        ssize_t ret = recv(_sockfd, buf, len, flag);
        if (ret <= 0) {
            if (errno == EAGAIN || errno == EINTR) return 0; // 暂时无数据
            return -1; // 连接关闭或错误
        }
        return ret;
    }

    ssize_t NonBlockRecv(void *buf, size_t len) {
        return Recv(buf, len, MSG_DONTWAIT);
    }

    ssize_t Send(const void *buf, size_t len, int flag = 0) {
        ssize_t ret = send(_sockfd, buf, len, flag);
        if (ret < 0) {
            if (errno == EAGAIN || errno == EINTR) return 0;
            return -1;
        }
        return ret;
    }

    ssize_t NonBlockSend(const void *buf, size_t len) {
        if (len == 0) return 0;
        return Send(buf, len, MSG_DONTWAIT);
    }

    /* ---------- 关闭 ---------- */
    void Close() {
        if (_sockfd != -1) { close(_sockfd); _sockfd = -1; }
    }

    /* ---------- 便捷工厂 ---------- */
    bool CreateServer(uint16_t port, const std::string &ip = "0.0.0.0", bool block_flag = false) {
        if (!Create()) return false;
        if (block_flag) SetNonBlock();
        if (!Bind(ip, port)) return false;
        if (!Listen())  return false;
        SetReuseAddr();
        return true;
    }

    bool CreateClient(uint16_t port, const std::string &ip) {
        if (!Create()) return false;
        if (!Connect(ip, port)) return false;
        return true;
    }

    void SetReuseAddr() {
        int val = 1;
        setsockopt(_sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
        setsockopt(_sockfd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));
    }

    void SetNonBlock() {
        int flag = fcntl(_sockfd, F_GETFL, 0);
        fcntl(_sockfd, F_SETFL, flag | O_NONBLOCK);
    }

private:
    int _sockfd;
};


/* ================================================================
 * 前向声明
 * ================================================================ */
class Poller;
class EventLoop;

/* ================================================================
 * Channel —— 文件描述符的事件管理器
 * 每个 fd 对应一个 Channel，注册到 EventLoop 的 Poller 中
 * ================================================================ */
class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop *loop, int fd)
        : _fd(fd), _events(0), _revents(0), _loop(loop) {}

    int Fd() { return _fd; }
    uint32_t Events() { return _events; }
    void SetREvents(uint32_t events) { _revents = events; }

    void SetReadCallback(const EventCallback &cb)  { _read_callback = cb; }
    void SetWriteCallback(const EventCallback &cb) { _write_callback = cb; }
    void SetErrorCallback(const EventCallback &cb) { _error_callback = cb; }
    void SetCloseCallback(const EventCallback &cb) { _close_callback = cb; }
    void SetEventCallback(const EventCallback &cb) { _event_callback = cb; }

    bool ReadAble()  { return (_events & EPOLLIN); }
    bool WriteAble() { return (_events & EPOLLOUT); }

    void EnableRead()   { _events |= EPOLLIN;  Update(); }
    void EnableWrite()  { _events |= EPOLLOUT; Update(); }
    void DisableRead()  { _events &= ~EPOLLIN;  Update(); }
    void DisableWrite() { _events &= ~EPOLLOUT; Update(); }
    void DisableAll()   { _events = 0;          Update(); }

    void Remove();
    void Update();

    // 事件分发：根据就绪的事件类型调用对应的回调
    void HandleEvent() {
        if (_revents & (EPOLLIN | EPOLLRDHUP | EPOLLPRI)) {
            if (_read_callback) _read_callback();
        }
        if (_revents & EPOLLOUT) {
            if (_write_callback) _write_callback();
        } else if (_revents & EPOLLERR) {
            if (_error_callback) _error_callback();
        } else if (_revents & EPOLLHUP) {
            if (_close_callback) _close_callback();
        }
        if (_event_callback) _event_callback();
    }

private:
    int _fd;
    EventLoop *_loop;
    uint32_t _events;     // 需要监控的事件（由用户设置）
    uint32_t _revents;    // 实际就绪的事件（由 Poller 设置）

    EventCallback _read_callback;
    EventCallback _write_callback;
    EventCallback _error_callback;
    EventCallback _close_callback;
    EventCallback _event_callback;
};


/* ================================================================
 * Poller —— epoll 事件监控封装
 * ================================================================ */
#define MAX_EPOLLEVENTS 1024

class Poller {
public:
    Poller() {
        _epfd = epoll_create(MAX_EPOLLEVENTS);
        if (_epfd < 0) { ERR_LOG("EPOLL CREATE FAILED!"); abort(); }
    }

    // 添加或修改事件监控
    void UpdateEvent(Channel *channel) {
        bool exists = _channels.find(channel->Fd()) != _channels.end();
        if (!exists) {
            _channels[channel->Fd()] = channel;
            EpollCtl(channel, EPOLL_CTL_ADD);
        } else {
            EpollCtl(channel, EPOLL_CTL_MOD);
        }
    }

    // 移除事件监控
    void RemoveEvent(Channel *channel) {
        _channels.erase(channel->Fd());
        EpollCtl(channel, EPOLL_CTL_DEL);
    }

    // 阻塞等待事件，将就绪的 Channel 放入 active
    void Poll(std::vector<Channel*> *active) {
        int nfds = epoll_wait(_epfd, _evs, MAX_EPOLLEVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) return;
            ERR_LOG("EPOLL WAIT ERROR: %s", strerror(errno));
            abort();
        }
        for (int i = 0; i < nfds; i++) {
            auto it = _channels.find(_evs[i].data.fd);
            assert(it != _channels.end());
            it->second->SetREvents(_evs[i].events);
            active->push_back(it->second);
        }
    }

private:
    void EpollCtl(Channel *channel, int op) {
        struct epoll_event ev;
        ev.data.fd = channel->Fd();
        ev.events  = channel->Events();
        if (epoll_ctl(_epfd, op, channel->Fd(), &ev) < 0) {
            ERR_LOG("EPOLLCTL FAILED!");
        }
    }

    int _epfd;
    struct epoll_event _evs[MAX_EPOLLEVENTS];
    std::unordered_map<int, Channel*> _channels;
};


/* ================================================================
 * TimerTask / TimerWheel —— 时间轮定时器
 * 使用 60 格的时间轮，每格代表 1 秒
 * ================================================================ */
using TaskFunc = std::function<void()>;
using ReleaseFunc = std::function<void()>;

class TimerTask {
public:
    TimerTask(uint64_t id, uint32_t delay, const TaskFunc &cb)
        : _id(id), _timeout(delay), _task_cb(cb), _canceled(false) {}

    // 析构时如果未被取消，执行定时任务（即定时器到期的处理）
    ~TimerTask() {
        if (!_canceled) _task_cb();
        _release();
    }

    void Cancel() { _canceled = true; }
    void SetRelease(const ReleaseFunc &cb) { _release = cb; }
    uint32_t DelayTime() { return _timeout; }

private:
    uint64_t _id;
    uint32_t _timeout;
    bool _canceled;
    TaskFunc _task_cb;
    ReleaseFunc _release;     // 删除 TimerWheel 中的弱引用
};

class TimerWheel {
public:
    TimerWheel(EventLoop *loop)
        : _capacity(60), _tick(0), _wheel(_capacity), _loop(loop)
        , _timerfd(CreateTimerfd())
        , _timer_channel(new Channel(_loop, _timerfd)) {
        _timer_channel->SetReadCallback(std::bind(&TimerWheel::OnTime, this));
        _timer_channel->EnableRead();
    }

    // 对外接口（线程安全：RunInLoop 转发）
    void TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb);
    void TimerRefresh(uint64_t id);
    void TimerCancel(uint64_t id);
    bool HasTimer(uint64_t id) {
        return _timers.find(id) != _timers.end();
    }

private:
    using WeakTask = std::weak_ptr<TimerTask>;
    using PtrTask  = std::shared_ptr<TimerTask>;

    // 创建 timerfd，每 1 秒触发一次
    static int CreateTimerfd() {
        int fd = timerfd_create(CLOCK_MONOTONIC, 0);
        if (fd < 0) { ERR_LOG("TIMERFD CREATE FAILED!"); abort(); }
        struct itimerspec it;
        it.it_value.tv_sec = 1;     it.it_value.tv_nsec = 0;
        it.it_interval.tv_sec = 1;  it.it_interval.tv_nsec = 0;
        timerfd_settime(fd, 0, &it, NULL);
        return fd;
    }

    int ReadTimefd() {
        uint64_t times = 0;
        if (read(_timerfd, &times, 8) < 0) {
            ERR_LOG("READ TIMERFD FAILED!"); abort();
        }
        return times;
    }

    void OnTime() {
        int times = ReadTimefd();   // 上次处理之后超时的次数
        for (int i = 0; i < times; i++) RunTimerTask();
    }

    // 秒针走一步：清空当前格，释放其中的 shared_ptr → 触发 TimerTask 析构 → 执行任务
    void RunTimerTask() {
        _tick = (_tick + 1) % _capacity;
        _wheel[_tick].clear();
    }

    // 以下三个 InLoop 函数在 EventLoop 线程中执行
    void TimerAddInLoop(uint64_t id, uint32_t delay, const TaskFunc &cb) {
        PtrTask pt(new TimerTask(id, delay, cb));
        pt->SetRelease(std::bind(&TimerWheel::RemoveTimer, this, id));
        int pos = (_tick + delay) % _capacity;
        _wheel[pos].push_back(pt);
        _timers[id] = WeakTask(pt);
    }

    void TimerRefreshInLoop(uint64_t id) {
        auto it = _timers.find(id);
        if (it == _timers.end()) return;
        PtrTask pt = it->second.lock();
        if (!pt) return;
        int pos = (_tick + pt->DelayTime()) % _capacity;
        _wheel[pos].push_back(pt);
    }

    void TimerCancelInLoop(uint64_t id) {
        auto it = _timers.find(id);
        if (it == _timers.end()) return;
        PtrTask pt = it->second.lock();
        if (pt) pt->Cancel();
    }

    void RemoveTimer(uint64_t id) { _timers.erase(id); }

    int _tick;
    int _capacity;
    std::vector<std::vector<PtrTask>> _wheel;
    std::unordered_map<uint64_t, WeakTask> _timers;

    EventLoop *_loop;
    int _timerfd;
    std::unique_ptr<Channel> _timer_channel;
};


/* ================================================================
 * EventLoop —— IO 事件循环（one loop per thread）
 * 模型：epoll_wait → 处理就绪事件 → 执行任务队列
 * ================================================================ */
class EventLoop {
public:
    using Functor = std::function<void()>;

    EventLoop()
        : _thread_id(std::this_thread::get_id())
        , _event_fd(CreateEventFd())
        , _event_channel(new Channel(this, _event_fd))
        , _timer_wheel(this) {
        _event_channel->SetReadCallback(std::bind(&EventLoop::ReadEventfd, this));
        _event_channel->EnableRead();
    }

    // 事件循环主函数（永不返回）
    void Start() {
        while (1) {
            std::vector<Channel*> actives;
            _poller.Poll(&actives);
            for (auto &ch : actives) ch->HandleEvent();
            RunAllTask();
        }
    }

    bool IsInLoop() { return _thread_id == std::this_thread::get_id(); }
    void AssertInLoop() { assert(_thread_id == std::this_thread::get_id()); }

    // 如果在当前线程则直接执行，否则压入任务队列
    void RunInLoop(const Functor &cb) {
        if (IsInLoop()) return cb();
        return QueueInLoop(cb);
    }

    void QueueInLoop(const Functor &cb) {
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _tasks.push_back(cb);
        }
        WeakUpEventFd();  // 唤醒 epoll_wait
    }

    // 事件管理转发
    void UpdateEvent(Channel *ch) { return _poller.UpdateEvent(ch); }
    void RemoveEvent(Channel *ch) { return _poller.RemoveEvent(ch); }

    // 定时器转发
    void TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb) {
        return _timer_wheel.TimerAdd(id, delay, cb);
    }
    void TimerRefresh(uint64_t id) { return _timer_wheel.TimerRefresh(id); }
    void TimerCancel(uint64_t id) { return _timer_wheel.TimerCancel(id); }
    bool HasTimer(uint64_t id) { return _timer_wheel.HasTimer(id); }

private:
    void RunAllTask() {
        std::vector<Functor> functor;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _tasks.swap(functor);
        }
        for (auto &f : functor) f();
    }

    /* ---------- eventfd：用于唤醒 epoll_wait 阻塞 ---------- */
    static int CreateEventFd() {
        int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0) { ERR_LOG("CREATE EVENTFD FAILED!"); abort(); }
        return efd;
    }

    void ReadEventfd() {
        uint64_t val = 0;
        int ret = read(_event_fd, &val, sizeof(val));
        if (ret < 0 && errno != EINTR && errno != EAGAIN) {
            ERR_LOG("READ EVENTFD FAILED!"); abort();
        }
    }

    void WeakUpEventFd() {
        uint64_t val = 1;
        if (write(_event_fd, &val, sizeof(val)) < 0 && errno != EINTR) {
            ERR_LOG("WRITE EVENTFD FAILED!"); abort();
        }
    }

    std::thread::id _thread_id;
    int _event_fd;
    std::unique_ptr<Channel> _event_channel;
    Poller _poller;
    std::vector<Functor> _tasks;
    std::mutex _mutex;
    TimerWheel _timer_wheel;
};


/* ================================================================
 * LoopThread —— 每个线程一个 EventLoop
 * ================================================================ */
class LoopThread {
public:
    LoopThread()
        : _loop(nullptr)
        , _thread(std::thread(&LoopThread::ThreadEntry, this)) {}

    // 等待 EventLoop 初始化完成并返回指针
    EventLoop *GetLoop() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cond.wait(lock, [this]() { return _loop != nullptr; });
        return _loop;
    }

private:
    void ThreadEntry() {
        EventLoop loop;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _loop = &loop;
            _cond.notify_all();
        }
        loop.Start();
    }

    std::mutex _mutex;
    std::condition_variable _cond;
    EventLoop *_loop;
    std::thread _thread;
};


/* ================================================================
 * LoopThreadPool —— EventLoop 线程池（IO 线程）
 * ================================================================ */
class LoopThreadPool {
public:
    LoopThreadPool(EventLoop *baseloop)
        : _thread_count(0), _next_idx(0), _baseloop(baseloop) {}

    void SetThreadCount(int count) { _thread_count = count; }

    void Create() {
        if (_thread_count <= 0) return;
        _threads.resize(_thread_count);
        _loops.resize(_thread_count);
        for (int i = 0; i < _thread_count; i++) {
            _threads[i] = new LoopThread();
            _loops[i] = _threads[i]->GetLoop();
        }
    }

    // 轮询选择下一个 EventLoop（负载均衡）
    EventLoop *NextLoop() {
        if (_thread_count == 0) return _baseloop;
        _next_idx = (_next_idx + 1) % _thread_count;
        return _loops[_next_idx];
    }

private:
    int _thread_count;
    int _next_idx;
    EventLoop *_baseloop;
    std::vector<LoopThread*> _threads;
    std::vector<EventLoop*> _loops;
};


/* ================================================================
 * Any —— 类型擦除容器（简易版 std::any）
 * 用于 Connection 的上下文存储
 * ================================================================ */
class Any {
public:
    Any() : _content(nullptr) {}

    template<class T>
    Any(const T &val) : _content(new PlaceHolder<T>(val)) {}

    Any(const Any &other)
        : _content(other._content ? other._content->Clone() : nullptr) {}

    ~Any() { delete _content; }

    template<class T>
    T *Get() {
        assert(typeid(T) == _content->Type());
        return &static_cast<PlaceHolder<T>*>(_content)->_val;
    }

    template<class T>
    Any& operator=(const T &val) {
        Any(val).Swap(*this);
        return *this;
    }

    Any& operator=(const Any &other) {
        Any(other).Swap(*this);
        return *this;
    }

    Any& Swap(Any &other) {
        std::swap(_content, other._content);
        return *this;
    }

private:
    struct Holder {
        virtual ~Holder() {}
        virtual const std::type_info& Type() = 0;
        virtual Holder *Clone() = 0;
    };

    template<class T>
    struct PlaceHolder : public Holder {
        PlaceHolder(const T &val) : _val(val) {}
        const std::type_info& Type() override { return typeid(T); }
        Holder *Clone() override { return new PlaceHolder(_val); }
        T _val;
    };

    Holder *_content;
};


/* ================================================================
 * Connection —— TCP 连接
 * 管理套接字、缓冲区、事件回调、状态机
 * ================================================================ */
enum ConnStatu { DISCONNECTED, CONNECTING, CONNECTED, DISCONNECTING };

class Connection;
using PtrConnection = std::shared_ptr<Connection>;

class Connection : public std::enable_shared_from_this<Connection> {
public:
    using ConnectedCallback = std::function<void(const PtrConnection&)>;
    using MessageCallback   = std::function<void(const PtrConnection&, Buffer*)>;
    using ClosedCallback    = std::function<void(const PtrConnection&)>;
    using AnyEventCallback  = std::function<void(const PtrConnection&)>;

    Connection(EventLoop *loop, uint64_t conn_id, int sockfd)
        : _conn_id(conn_id), _sockfd(sockfd)
        , _enable_inactive_release(false), _loop(loop)
        , _statu(CONNECTING), _socket(_sockfd), _channel(loop, _sockfd) {
        _channel.SetCloseCallback(std::bind(&Connection::HandleClose, this));
        _channel.SetEventCallback(std::bind(&Connection::HandleEvent, this));
        _channel.SetReadCallback(std::bind(&Connection::HandleRead, this));
        _channel.SetWriteCallback(std::bind(&Connection::HandleWrite, this));
        _channel.SetErrorCallback(std::bind(&Connection::HandleError, this));
    }

    ~Connection() { DBG_LOG("RELEASE CONNECTION:%p", this); }

    int  Fd() { return _sockfd; }
    int  Id() { return _conn_id; }
    bool Connected() { return (_statu == CONNECTED); }

    void SetContext(const Any &context) { _context = context; }
    Any *GetContext() { return &_context; }

    void SetConnectedCallback(const ConnectedCallback &cb) { _connected_callback = cb; }
    void SetMessageCallback(const MessageCallback &cb)     { _message_callback = cb; }
    void SetClosedCallback(const ClosedCallback &cb)       { _closed_callback = cb; }
    void SetAnyEventCallback(const AnyEventCallback &cb)   { _event_callback = cb; }
    void SetSrvClosedCallback(const ClosedCallback &cb)    { _server_closed_callback = cb; }

    // 连接建立就绪：设置回调、启动读监控、调用 _connected_callback
    void Established() {
        _loop->RunInLoop(std::bind(&Connection::EstablishedInLoop, this));
    }

    // 发送数据（线程安全）
    void Send(const char *data, size_t len) {
        Buffer buf;
        buf.WriteAndPush(data, len);
        _loop->RunInLoop(std::bind(&Connection::SendInLoop, this, std::move(buf)));
    }

    // 优雅关闭（等数据发完再关，线程安全）
    void Shutdown() {
        _loop->RunInLoop(std::bind(&Connection::ShutdownInLoop, this));
    }

    // 立即释放（线程安全）
    void Release() {
        _loop->QueueInLoop(std::bind(&Connection::ReleaseInLoop, this));
    }

    void EnableInactiveRelease(int sec) {
        _loop->RunInLoop(std::bind(&Connection::EnableInactiveReleaseInLoop, this, sec));
    }

    void CancelInactiveRelease() {
        _loop->RunInLoop(std::bind(&Connection::CancelInactiveReleaseInLoop, this));
    }

    // 协议升级（必须在 EventLoop 线程中调用）
    void Upgrade(const Any &context,
                 const ConnectedCallback &conn,
                 const MessageCallback &msg,
                 const ClosedCallback &closed,
                 const AnyEventCallback &event) {
        _loop->AssertInLoop();
        _loop->RunInLoop(std::bind(&Connection::UpgradeInLoop, this,
                                   context, conn, msg, closed, event));
    }

private:
    /* ====== Channel 事件回调（在 EventLoop 线程中执行） ====== */

    // 可读事件：接收数据 → 调用 _message_callback
    void HandleRead() {
        char buf[65536];
        ssize_t ret = _socket.NonBlockRecv(buf, sizeof(buf) - 1);
        if (ret < 0) {
            // ret == -1：连接已关闭或发生错误
            // 先处理已有数据再关闭
            if (_in_buffer.ReadAbleSize() > 0) {
                _message_callback(shared_from_this(), &_in_buffer);
            }
            return ShutdownInLoop();
        }
        if (ret == 0) return;  // EAGAIN，暂时无数据

        _in_buffer.WriteAndPush(buf, ret);
        if (_in_buffer.ReadAbleSize() > 0) {
            _message_callback(shared_from_this(), &_in_buffer);
        }
    }

    // 可写事件：发送缓冲区中的数据
    void HandleWrite() {
        ssize_t ret = _socket.NonBlockSend(
            _out_buffer.ReadPosition(), _out_buffer.ReadAbleSize());
        if (ret < 0) {
            // 发送失败则释放连接
            if (_in_buffer.ReadAbleSize() > 0) {
                _message_callback(shared_from_this(), &_in_buffer);
            }
            return Release();
        }
        _out_buffer.MoveReadOffset(ret);
        if (_out_buffer.ReadAbleSize() == 0) {
            _channel.DisableWrite();
            if (_statu == DISCONNECTING) return Release();
        }
    }

    // 挂断事件
    void HandleClose() {
        if (_in_buffer.ReadAbleSize() > 0) {
            _message_callback(shared_from_this(), &_in_buffer);
        }
        return Release();
    }

    // 错误事件
    void HandleError() { return HandleClose(); }

    // 任意事件：刷新定时器 + 调用用户回调
    void HandleEvent() {
        if (_enable_inactive_release) _loop->TimerRefresh(_conn_id);
        if (_event_callback) _event_callback(shared_from_this());
    }

    /* ====== InLoop 操作 ====== */

    void EstablishedInLoop() {
        assert(_statu == CONNECTING);
        _statu = CONNECTED;
        _channel.EnableRead();
        if (_connected_callback) _connected_callback(shared_from_this());
    }

    void ReleaseInLoop() {
        _statu = DISCONNECTED;
        _channel.Remove();
        _socket.Close();
        if (_loop->HasTimer(_conn_id)) CancelInactiveReleaseInLoop();
        // 先调用用户回调再移除服务器管理信息
        if (_closed_callback) _closed_callback(shared_from_this());
        if (_server_closed_callback) _server_closed_callback(shared_from_this());
    }

    void SendInLoop(Buffer &buf) {
        if (_statu == DISCONNECTED) return;
        _out_buffer.WriteBufferAndPush(buf);
        if (!_channel.WriteAble()) _channel.EnableWrite();
    }

    void ShutdownInLoop() {
        _statu = DISCONNECTING;
        if (_in_buffer.ReadAbleSize() > 0 && _message_callback) {
            _message_callback(shared_from_this(), &_in_buffer);
        }
        if (_out_buffer.ReadAbleSize() > 0) {
            if (!_channel.WriteAble()) _channel.EnableWrite();
        }
        if (_out_buffer.ReadAbleSize() == 0) Release();
    }

    void EnableInactiveReleaseInLoop(int sec) {
        _enable_inactive_release = true;
        if (_loop->HasTimer(_conn_id)) {
            return _loop->TimerRefresh(_conn_id);
        }
        _loop->TimerAdd(_conn_id, sec,
                        std::bind(&Connection::Release, this));
    }

    void CancelInactiveReleaseInLoop() {
        _enable_inactive_release = false;
        if (_loop->HasTimer(_conn_id)) _loop->TimerCancel(_conn_id);
    }

    void UpgradeInLoop(const Any &context,
                       const ConnectedCallback &conn,
                       const MessageCallback &msg,
                       const ClosedCallback &closed,
                       const AnyEventCallback &event) {
        _context = context;
        _connected_callback = conn;
        _message_callback = msg;
        _closed_callback = closed;
        _event_callback = event;
    }

    uint64_t _conn_id;
    int _sockfd;
    bool _enable_inactive_release;
    EventLoop *_loop;
    ConnStatu _statu;
    Socket _socket;
    Channel _channel;
    Buffer _in_buffer;    // 输入缓冲区
    Buffer _out_buffer;   // 输出缓冲区
    Any _context;

    ConnectedCallback _connected_callback;
    MessageCallback _message_callback;
    ClosedCallback _closed_callback;
    AnyEventCallback _event_callback;
    ClosedCallback _server_closed_callback;   // 服务器内部管理
};


/* ================================================================
 * Acceptor —— 监听套接字管理器
 * ================================================================ */
class Acceptor {
public:
    using AcceptCallback = std::function<void(int)>;

    Acceptor(EventLoop *loop, int port)
        : _loop(loop), _channel(loop, _socket.Fd()) {
        // 注意：必须先创建监听套接字再初始化 _channel
        _socket.CreateServer(port);
        _channel = Channel(loop, _socket.Fd());
        _channel.SetReadCallback(std::bind(&Acceptor::HandleRead, this));
    }

    void SetAcceptCallback(const AcceptCallback &cb) { _accept_callback = cb; }
    void Listen() { _channel.EnableRead(); }

private:
    void HandleRead() {
        int newfd = _socket.Accept();
        if (newfd >= 0 && _accept_callback) _accept_callback(newfd);
    }

    Socket _socket;
    EventLoop *_loop;
    Channel _channel;
    AcceptCallback _accept_callback;
};


/* ================================================================
 * TcpServer —— Reactor TCP 服务器
 *
 * 使用方式：
 *   TcpServer server(port);
 *   server.SetThreadCount(2);                      // IO 线程数
 *   server.EnableInactiveRelease(10);              // 10s 非活跃断开
 *   server.SetConnectedCallback(OnConnected);
 *   server.SetMessageCallback(OnMessage);
 *   server.SetClosedCallback(OnClosed);
 *   server.Start();                                 // 启动
 * ================================================================ */
class TcpServer {
public:
    using ConnectedCallback = std::function<void(const PtrConnection&)>;
    using MessageCallback   = std::function<void(const PtrConnection&, Buffer*)>;
    using ClosedCallback    = std::function<void(const PtrConnection&)>;
    using AnyEventCallback  = std::function<void(const PtrConnection&)>;
    using Functor           = std::function<void()>;

    TcpServer(int port)
        : _next_id(0)
        , _port(port)
        , _timeout(0)
        , _enable_inactive_release(false)
        , _acceptor(&_baseloop, port)
        , _pool(&_baseloop) {
        _acceptor.SetAcceptCallback(
            std::bind(&TcpServer::NewConnection, this, std::placeholders::_1));
        _acceptor.Listen();
    }

    void SetThreadCount(int count)           { _pool.SetThreadCount(count); }
    void SetConnectedCallback(const ConnectedCallback &cb) { _connected_callback = cb; }
    void SetMessageCallback(const MessageCallback &cb)     { _message_callback = cb; }
    void SetClosedCallback(const ClosedCallback &cb)       { _closed_callback = cb; }
    void SetAnyEventCallback(const AnyEventCallback &cb)   { _event_callback = cb; }
    void EnableInactiveRelease(int timeout) {
        _timeout = timeout;
        _enable_inactive_release = true;
    }

    void RunAfter(const Functor &task, int delay) {
        _baseloop.RunInLoop(
            std::bind(&TcpServer::RunAfterInLoop, this, task, delay));
    }

    void Start() { _pool.Create(); _baseloop.Start(); }

private:
    void RunAfterInLoop(const Functor &task, int delay) {
        _next_id++;
        _baseloop.TimerAdd(_next_id, delay, task);
    }

    // 接受新连接
    void NewConnection(int fd) {
        _next_id++;
        PtrConnection conn(new Connection(_pool.NextLoop(), _next_id, fd));
        conn->SetMessageCallback(_message_callback);
        conn->SetClosedCallback(_closed_callback);
        conn->SetConnectedCallback(_connected_callback);
        conn->SetAnyEventCallback(_event_callback);
        conn->SetSrvClosedCallback(
            std::bind(&TcpServer::RemoveConnection, this, std::placeholders::_1));
        if (_enable_inactive_release) conn->EnableInactiveRelease(_timeout);
        conn->Established();
        _conns[_next_id] = conn;
    }

    void RemoveConnection(const PtrConnection &conn) {
        _baseloop.RunInLoop(
            std::bind(&TcpServer::RemoveConnectionInLoop, this, conn));
    }

    void RemoveConnectionInLoop(const PtrConnection &conn) {
        _conns.erase(conn->Id());
    }

    uint64_t _next_id;
    int _port;
    int _timeout;
    bool _enable_inactive_release;
    EventLoop _baseloop;
    Acceptor _acceptor;
    LoopThreadPool _pool;
    std::unordered_map<uint64_t, PtrConnection> _conns;

    ConnectedCallback _connected_callback;
    MessageCallback _message_callback;
    ClosedCallback _closed_callback;
    AnyEventCallback _event_callback;
};


/* ================================================================
 * 类外定义的成员函数（因涉及完整类型不得不放在末尾）
 * ================================================================ */

void Channel::Remove() { return _loop->RemoveEvent(this); }
void Channel::Update() { return _loop->UpdateEvent(this); }

void TimerWheel::TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb) {
    _loop->RunInLoop(std::bind(&TimerWheel::TimerAddInLoop, this, id, delay, cb));
}
void TimerWheel::TimerRefresh(uint64_t id) {
    _loop->RunInLoop(std::bind(&TimerWheel::TimerRefreshInLoop, this, id));
}
void TimerWheel::TimerCancel(uint64_t id) {
    _loop->RunInLoop(std::bind(&TimerWheel::TimerCancelInLoop, this, id));
}


/* ================================================================
 * NetWork —— 全局网络初始化
 * 忽略 SIGPIPE 信号（防止写入已关闭的连接时进程退出）
 * ================================================================ */
class NetWork {
public:
    NetWork() {
        signal(SIGPIPE, SIG_IGN);
        DBG_LOG("SIGPIPE IGNORED");
    }
};
static NetWork nw;

#endif // __M_SERVER_H__

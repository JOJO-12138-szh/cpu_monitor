# 环形缓冲区 (Ring Buffer) 实现详解

## 1. 概述

环形缓冲区（Ring Buffer）是一种**固定大小的 FIFO 队列**，利用数组和头尾指针循环使用空间。本实现具备以下特点：

- **有界队列**：容量固定，满时拒绝写入。
- **线程安全**：使用互斥锁保护所有操作，支持多线程并发访问。
- **阻塞与超时获取**：消费者在队列为空时可阻塞等待，支持超时返回。

## 2. 数据结构定义

```c
// 单条 CPU 采样记录
typedef struct {
    char   cpu_id[16];    // CPU 标识
    double usage_rate;    // 使用率百分比
    time_t sample_time;   // 采样时间戳
} cpu_sample_t;

// 环形缓冲区结构体
typedef struct {
    cpu_sample_t *buf;    // 数据缓冲区（数组）
    size_t cap;           // 总容量
    size_t head;          // 读索引（下一个要取出的位置）
    size_t tail;          // 写索引（下一个要写入的位置）
    size_t count;         // 当前元素数量
    pthread_mutex_t lock; // 互斥锁
    pthread_cond_t  not_empty; // 条件变量：消费者等待数据时使用
} ring_t;
```
head 和 tail 通过 % cap 实现循环移动。

count 用于快速判断空/满，避免歧义（当 head == tail 时可能是空也可能是满）。

## 3. 核心操作实现
### 3.1 初始化与销毁
```c
int rb_init(ring_t *r, size_t cap) {
    r->buf = calloc(cap, sizeof(cpu_sample_t));  // 分配并清零缓冲区内存
    if (!r->buf) return -1;                      // 内存分配失败
    r->cap = cap;                                // 设置容量
    r->head = r->tail = r->count = 0;            // 初始化指针和计数为0
    pthread_mutex_init(&r->lock, NULL);           // 初始化互斥锁
    pthread_cond_init(&r->not_empty, NULL);      // 初始化条件变量
    return 0;
}

void rb_destroy(ring_t *r) {
    pthread_mutex_destroy(&r->lock);       // 销毁互斥锁
    pthread_cond_destroy(&r->not_empty);   // 销毁条件变量
    free(r->buf);                          // 释放缓冲区内存
}
```
初始化时分配 cap 个元素的内存，并清零。

销毁时释放锁、条件变量和内存。

### 3.2 入队操作（生产者调用）
```c
/* 生产者函数：向缓冲区写入一条数据
   参数：r-缓冲区指针，s-要写入的数据指针
   返回：0成功，-1表示队列满写入失败
   注意：队列满时立即返回-1，不阻塞，调用方需要兜底处理 */
int rb_push(ring_t *r, const cpu_sample_t *s) {
    int ret = 0;
    pthread_mutex_lock(&r->lock);        // 加锁，保护缓冲区数据
    if (r->count == r->cap) {            // 判断队列是否已满
        ret = -1;                        // 已满，设置返回值为-1
    } else {
        r->buf[r->tail] = *s;                    // 将数据复制到tail位置
        r->tail = (r->tail + 1) % r->cap;        // tail指针循环后移
        r->count++;                               // 元素计数加1
        pthread_cond_signal(&r->not_empty);       // 发送信号唤醒一个等待的消费者
    }
    pthread_mutex_unlock(&r->lock);      // 解锁
    return ret;
}
```
关键点：

入队前必须获取锁。

检查 count == cap：若满则立即返回 -1，由调用方做兜底处理（如写本地日志）。

若未满，将数据复制到 buf[tail] 并更新指针和计数。

发送条件变量信号 pthread_cond_signal，唤醒一个正在等待 not_empty 的消费者线程。

解锁后返回结果。

为什么满时不阻塞？
生产者通常是采样线程，不应因缓冲满而阻塞采样；丢弃并写本地日志可保证程序实时性。

### 3.3 出队操作（消费者调用）
```c
/* 消费者函数：从缓冲区批量取出数据
   参数：r-缓冲区指针，out-输出数组，max-最多取多少条，timeout_ms-等待超时(毫秒)
   返回：实际取出的数据条数(可能为0)
   实现双阈值：1)取满max条 2)等待超时 任一条件满足即返回 */
size_t rb_pop(ring_t *r, cpu_sample_t *out, size_t max, int timeout_ms) {
    size_t n = 0;                        // 已取出的数据计数
    struct timespec ts;                  // 绝对超时时间
    clock_gettime(CLOCK_REALTIME, &ts);  // 获取当前时间
    ts.tv_sec += timeout_ms;            //超时按秒加, 无溢出问题

    pthread_mutex_lock(&r->lock);        // 加锁
    // 当缓冲区为空时循环等待，直到有数据或超时
    while (r->count == 0)
        if (pthread_cond_timedwait(&r->not_empty, &r->lock, &ts) == ETIMEDOUT)
            break;                       // 等待超时，跳出循环
    // 从head开始取数据，直到取满max条或缓冲区为空
    while (n < max && r->count > 0) {
        out[n++] = r->buf[r->head];               // 取出head位置的数据
        r->head = (r->head + 1) % r->cap;         // head指针循环后移
        r->count--;                                // 元素计数减1
    }
    pthread_mutex_unlock(&r->lock);      // 解锁
    return n;                            // 返回实际取出的条数
}

```
参数说明：

out：存放取出数据的数组。

max：最多取出的条数。

timeout_ms：最长等待时间（毫秒）。

执行流程：

计算绝对超时时间点（当前时间 + timeout_ms）。

加锁后进入循环：当缓冲区为空时，调用 pthread_cond_timedwait 等待。该函数会原子地释放锁并阻塞，直到：

被 pthread_cond_signal 唤醒（有新数据），

或超时（返回 ETIMEDOUT）。

被唤醒或超时后，重新获得锁，继续检查条件。如果超时则跳出等待循环。

实际取出数据：从 head 开始逐个复制到 out 数组，并移动 head、递减 count，直到取够 max 条或缓冲区变空。

解锁并返回实际取出的条数。

双阈值触发：
消费者可传入 max = 缓存容量 和 timeout_ms = 时间阈值，实现：

条数阈值：当累积数据达到 max 条时，一次取走全部。

时间阈值：即使数据量不足 max，等待 timeout_ms 后也会返回已有数据（可能为0），避免无限等待。



## 4. 知识点
### 4.1 锁的粒度
所有对缓冲区内部状态（head, tail, count, buf）的读写都在锁保护下进行，确保了原子性。

### 4.3 生产-消费并发模型
生产者：只在 rb_push 中短暂持锁，写入后立即释放，不等待消费者。

消费者：在队列空时释放锁并阻塞，不浪费 CPU；有新数据或超时后立即工作。

两者通过条件变量 not_empty 协调，生产者发送信号，消费者响应信号。

#define _GNU_SOURCE
#include <errno.h>
#include <mysql/mysql.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ================= 配置 ================= */
#define DB_HOST     "127.0.0.1"   // 数据库主机地址
#define DB_USER     "tim"        // 数据库用户名
#define DB_PASS     "12345"      // 数据库密码
#define DB_NAME     "hr"         // 数据库名
#define DB_PORT     3306         // 数据库端口

#define RING_CAP    1024        /* 环形缓冲容量(条数阈值)：缓冲区最多缓存1024条记录 */
#define FLUSH_MS    1           /* 时间阈值: 每隔1毫秒从缓冲区取一次数据（实际就是立即返回） */
#define INTERVAL_MS 500         /* CPU 采样间隔：每500毫秒采集一次CPU使用率 */

/* ================= 环形缓冲区 ================= */
// CPU采样数据结构体：存储单次采样的完整信息
typedef struct {
    char   cpu_id[16];         // CPU标识符，如"cpu"表示总CPU
    double usage_rate;         // CPU使用率，0.0~100.0
    time_t sample_time;        // 采样时间戳
} cpu_sample_t;

// 环形缓冲区结构体：生产者-消费者模型的共享队列
typedef struct {
    cpu_sample_t *buf;         // 指向缓冲区数组的指针
    size_t cap;                // 缓冲区总容量
    size_t head;               // 读指针：下次读取的起始位置
    size_t tail;               // 写指针：下次写入的起始位置
    size_t count;              // 当前缓冲区中的元素个数
    pthread_mutex_t lock;      // 互斥锁：保护所有成员变量的访问
    pthread_cond_t  not_empty; // 条件变量：消费者等待数据时使用
} ring_t;

// 初始化环形缓冲区
// 参数：r-缓冲区指针，cap-容量
// 返回：0成功，-1失败
int rb_init(ring_t *r, size_t cap) {
    r->buf = calloc(cap, sizeof(cpu_sample_t));  // 分配并清零缓冲区内存
    if (!r->buf) return -1;                      // 内存分配失败
    r->cap = cap;                                // 设置容量
    r->head = r->tail = r->count = 0;            // 初始化指针和计数为0
    pthread_mutex_init(&r->lock, NULL);           // 初始化互斥锁
    pthread_cond_init(&r->not_empty, NULL);      // 初始化条件变量
    return 0;
}

// 销毁环形缓冲区，释放所有资源
void rb_destroy(ring_t *r) {
    pthread_mutex_destroy(&r->lock);       // 销毁互斥锁
    pthread_cond_destroy(&r->not_empty);   // 销毁条件变量
    free(r->buf);                          // 释放缓冲区内存
}

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

/* 消费者函数：从缓冲区批量取出数据
   参数：r-缓冲区指针，out-输出数组，max-最多取多少条，timeout_ms-等待超时(毫秒)
   返回：实际取出的数据条数(可能为0)
   注意：本版本中timeout_ms直接加到秒上，未处理纳秒部分（简化版） */
size_t rb_pop(ring_t *r, cpu_sample_t *out, size_t max, int timeout_ms) {
    size_t n = 0;                        // 已取出的数据计数
    struct timespec ts;                  // 绝对超时时间
    clock_gettime(CLOCK_REALTIME, &ts);  // 获取当前时间
    ts.tv_sec += timeout_ms;             // 直接将毫秒加到秒上（简化处理，实际应转换）

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

/* ================= 全局变量和辅助函数 ================= */
static volatile sig_atomic_t g_stop = 0;  // 全局停止标志，volatile确保信号处理函数修改后主循环可见
                                          // sig_atomic_t保证读写是原子操作

// 信号处理函数：接收SIGINT(Ctrl+C)或SIGTERM信号时设置停止标志
void on_signal(int sig) 
{ 
    (void)sig; g_stop = 1;  // 忽略信号值，直接设置全局停止标志
}

// 兜底函数：当缓冲区满或数据库不可用时，将CPU数据写入本地文件防止丢失
void log_fallback(const cpu_sample_t *s) {
    FILE *f = fopen("cpu_fallback.log", "a");  // 以追加模式打开日志文件
    if (!f) return;                             // 打开失败则放弃
    struct tm tm;
    localtime_r(&s->sample_time, &tm);          // 将时间戳转换为本地时间结构
    // 格式化写入：年月日 时分秒 cpu_id 使用率
    fprintf(f, "%04d-%02d-%02d %02d:%02d:%02d %s %.3f\n",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec, s->cpu_id, s->usage_rate);
    fclose(f);
}

/* ================= 采集线程: 读 /proc/stat 计算CPU使用率 ================= */
static ring_t g_rb;  // 全局环形缓冲区实例

// 采集线程主函数：周期性读取/proc/stat，计算CPU总使用率
void *collector(void *arg) {
    (void)arg;
    FILE *fp;
    char name[16];
    unsigned long long u, n, s, idl, iow, irq, si;
    unsigned long long tp = 0, wp = 0;   /* tp:上次总CPU时间, wp:上次工作时间(总-idle-iowait) */
    unsigned long long tc = 0, wc = 0;   /* tc:本次总CPU时间, wc:本次工作时间 */

    // 第一次读取/proc/stat获取基准值
    fp = fopen("/proc/stat", "r");
    if (!fp) { perror("/proc/stat"); return NULL; }
    // 解析第一行"cpu"的各个时间片: user nice system idle iowait irq softirq
    if (fscanf(fp, "%15s %llu %llu %llu %llu %llu %llu %llu", name,
               &u, &n, &s, &idl, &iow, &irq, &si) >= 4) {
        tp = u + n + s + idl + iow + irq + si;  // 总时间 = 所有时间片之和
        wp = tp - (idl + iow);                  // 工作时间 = 总时间 - 空闲 - IO等待
    }
    fclose(fp);

    // 主采集循环
    while (!g_stop) {
        usleep(INTERVAL_MS * 1000);             // 休眠500毫秒
        fp = fopen("/proc/stat", "r");
        if (!fp) continue;                      // 打开失败则跳过本次采样
        // 再次读取/proc/stat获取新值
        if (fscanf(fp, "%15s %llu %llu %llu %llu %llu %llu %llu", name,
                   &u, &n, &s, &idl, &iow, &irq, &si) >= 4) {
            tc = u + n + s + idl + iow + irq + si;
            wc = tc - (idl + iow);
        }
        fclose(fp);

        if (tc <= tp) continue;  // 防止时间倒退或除零错误

        // CPU使用率 = (本次工作时间-上次工作时间) / (本次总时间-上次总时间) * 100
        double usage = (double)(wc - wp) / (double)(tc - tp) * 100.0;
        // 边界值检查，确保使用率在0~100之间
        if (usage < 0) usage = 0;
        if (usage > 100) usage = 100;

        // 构造采样记录
        cpu_sample_t e = { "cpu", usage, time(NULL) };
        // 尝试推入环形缓冲区，失败则写本地日志兜底
        if (rb_push(&g_rb, &e) != 0) log_fallback(&e);
        // 更新基准值为下一次计算做准备
        tp = tc;
        wp = wc;
    }
    return NULL;
}

/* ================= 写库线程: 使用预编译语句批量插入MySQL ================= */
static void *db_writer(void *arg) {
    (void)arg;
    cpu_sample_t batch[RING_CAP];    // 批量缓冲区：一次最多处理1024条
    MYSQL *conn = NULL;              // MySQL连接句柄
    MYSQL_STMT *stmt = NULL;         // 预编译语句句柄
    MYSQL_BIND bind[2];             // 绑定参数数组
    MYSQL_TIME mt;                  // MySQL时间结构体
    double usage;                   // CPU使用率（绑定到预编译语句）
    unsigned long i, n;

    // 外层循环：数据库重连机制
    for (;;) {
        if (g_stop) break;          // 收到停止信号则退出

        // 尝试连接数据库
        conn = mysql_init(NULL);
        if (!conn) { sleep(3); continue; }  // 初始化失败，等待3秒后重试
        if (!mysql_real_connect(conn, DB_HOST, DB_USER, DB_PASS,
                                DB_NAME, DB_PORT, NULL, 0)) {
            fprintf(stderr, "[db] connect fail: %s\n", mysql_error(conn));
            mysql_close(conn);
            conn = NULL;
            sleep(3);               // 连接失败，等待3秒后重试
            continue;
        }

        // 自动建表（如果表不存在则创建）
        const char *ddl =
            "CREATE TABLE IF NOT EXISTS cpu_usage ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"  // 自增主键
            "  sample_time DATETIME     NOT NULL,"           // 采样时间
            "  cpu_id      VARCHAR(16)  NOT NULL,"           // CPU标识
            "  usage_rate  DECIMAL(6,3) NOT NULL,"           // 使用率(精确到3位小数)
            "  PRIMARY KEY (id),"
            "  KEY idx_sample_time (sample_time)"            // 时间索引加速查询
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        if (mysql_real_query(conn, ddl, strlen(ddl)) != 0) {
            fprintf(stderr, "[db] create table fail: %s\n", mysql_error(conn));
            goto reconnect;         // 建表失败，跳转清理资源后重连
        }
        mysql_autocommit(conn, 0);  // 关闭自动提交，使用手动事务控制

        // 准备预编译INSERT语句（?为占位符）
        const char *sql =
            "INSERT INTO cpu_usage(sample_time, cpu_id, usage_rate) VALUES(?,?,?)";
        stmt = mysql_stmt_init(conn);
        if (!stmt || mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
            fprintf(stderr, "[db] prepare fail: %s\n", mysql_stmt_error(stmt));
            goto reconnect;
        }

        // 绑定预编译语句的参数
        memset(bind, 0, sizeof(bind));          // 清零绑定结构体
        bind[0].buffer_type = MYSQL_TYPE_DATETIME;  // 第一个参数：DATETIME类型
        bind[0].buffer      = (char *)&mt;          // 指向时间结构体
        bind[0].buffer_length = sizeof(MYSQL_TIME); // 时间结构体大小
        bind[1].buffer_type = MYSQL_TYPE_DOUBLE;    // 第二个参数：DOUBLE类型
        bind[1].buffer      = (void *)&usage;       // 指向使用率变量
        if (mysql_stmt_bind_param(stmt, bind) != 0) {
            fprintf(stderr, "[db] bind fail: %s\n", mysql_stmt_error(stmt));
            goto reconnect;
        }

        // 内层循环：从环形缓冲区取数据并批量写入数据库
        // FLUSH_MS=1表示几乎立即返回（有数据就取，没数据等1ms后超时返回）
        while (!g_stop) {
            // 从缓冲区取数据，最多取1024条，最多等待1毫秒
            n = rb_pop(&g_rb, batch, RING_CAP, FLUSH_MS);
            if (n == 0) continue;   // 没取到数据，继续循环

            // 逐条执行插入操作
            for (i = 0; i < n; i++) {
                struct tm tm;
                localtime_r(&batch[i].sample_time, &tm);  // 时间戳转本地时间
                // 填充MySQL时间结构体
                mt.year = tm.tm_year + 1900;
                mt.month = tm.tm_mon + 1;
                mt.day   = tm.tm_mday;
                mt.hour = tm.tm_hour;
                mt.minute = tm.tm_min;
                mt.second = tm.tm_sec;
                mt.second_part = 0;  // 微秒部分为0
                mt.neg = 0;          // 非负数

                usage = batch[i].usage_rate;  // 设置使用率参数
                // 执行预编译语句（参数已通过绑定变量传入）
                if (mysql_stmt_execute(stmt) != 0) {
                    fprintf(stderr, "[db] execute fail: %s\n", mysql_stmt_error(stmt));
                    mysql_rollback(conn);   // 执行失败，回滚本批次
                    goto reconnect;         // 跳转清理资源后重连
                }
            }
            mysql_commit(conn);  // 本批次全部成功，提交事务
        }

reconnect:
        // 清理数据库连接资源
        if (stmt) { mysql_stmt_close(stmt); stmt = NULL; }
        if (conn) { mysql_close(conn); conn = NULL; }
    }

    // 程序退出前的收尾工作：处理缓冲区剩余数据
    // 此时采集线程已停止，使用timeout=0非阻塞取数
    while ((n = rb_pop(&g_rb, batch, RING_CAP, 0)) > 0) {
        // 重新连接数据库
        conn = mysql_init(NULL);
        if (!conn || !mysql_real_connect(conn, DB_HOST, DB_USER, DB_PASS,
                                        DB_NAME, DB_PORT, NULL, 0)) {
            // 连接失败，剩余数据全部写本地日志兜底
            for (i = 0; i < n; i++)
                log_fallback(&batch[i]);
            if (conn) mysql_close(conn);
            break;
        }

        mysql_autocommit(conn, 0);  // 关闭自动提交
        stmt = mysql_stmt_init(conn);
        const char *sql =
            "INSERT INTO cpu_usage(sample_time, cpu_id, usage_rate) VALUES(?,?,?)";
        mysql_stmt_prepare(stmt, sql, strlen(sql));

        // 重新绑定参数（新连接需要重新设置）
        memset(bind, 0, sizeof(bind));
        bind[0].buffer_type = MYSQL_TYPE_DATETIME;
        bind[0].buffer = (char *)&mt;
        bind[0].buffer_length = sizeof(MYSQL_TIME);
        bind[1].buffer_type = MYSQL_TYPE_DOUBLE;
        bind[1].buffer = (void *)&usage;
        mysql_stmt_bind_param(stmt, bind);

        // 逐条插入剩余数据
        for (i = 0; i < n; i++) {
            struct tm tm;
            localtime_r(&batch[i].sample_time, &tm);
            mt.year = tm.tm_year + 1900;
            mt.month = tm.tm_mon + 1;
            mt.day = tm.tm_mday;
            mt.hour = tm.tm_hour;
            mt.minute = tm.tm_min;
            mt.second = tm.tm_sec;
            mt.second_part = 0;
            mt.neg = 0;
            usage = batch[i].usage_rate;
            // 单条插入失败则写本地日志
            if (mysql_stmt_execute(stmt) != 0) {
                log_fallback(&batch[i]);
            }
        }
        mysql_commit(conn);          // 提交事务
        mysql_stmt_close(stmt);      // 关闭预编译语句
        mysql_close(conn);           // 关闭连接
    }
    return NULL;
}

/* ================= 主函数 ================= */
int main(void) {
    // 初始化环形缓冲区
    if (rb_init(&g_rb, RING_CAP) != 0) {
        fprintf(stderr, "ring buffer init fail\n");
        return 1;
    }

    // 注册信号处理函数
    signal(SIGINT, on_signal);   // 处理Ctrl+C
    signal(SIGTERM, on_signal);  // 处理kill命令

    pthread_t t1, t2;
    // 创建采集线程
    pthread_create(&t1, NULL, collector, NULL);
    // 创建写库线程
    pthread_create(&t2, NULL, db_writer, NULL);

    // 等待采集线程结束（收到停止信号后退出循环）
    pthread_join(t1, NULL);
    // 等待写库线程结束（处理完剩余数据后退出）
    pthread_join(t2, NULL);

    // 清理资源
    rb_destroy(&g_rb);
    printf("done.\n");
    return 0;
}
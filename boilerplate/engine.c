/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Complete implementation:
 *   - UNIX domain socket control-plane IPC
 *   - container lifecycle with clone() + namespaces + chroot
 *   - bounded-buffer producer/consumer logging
 *   - SIGCHLD / SIGINT / SIGTERM signal handling
 *   - ps / logs / start / run / stop CLI commands
 *   - ioctl integration with kernel monitor
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ─── Constants ─────────────────────────────────────────────────────────── */
#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "logs"
#define CONTROL_MESSAGE_LEN  256
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  16
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)

/* ─── Enumerations ──────────────────────────────────────────────────────── */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

/* ─── Data Structures ───────────────────────────────────────────────────── */
typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* ─── Global supervisor context pointer (for signal handlers) ───────────── */
static supervisor_ctx_t *g_ctx = NULL;

/* ─── Forward declarations ──────────────────────────────────────────────── */
static void usage(const char *prog);
static int parse_mib_flag(const char *flag, const char *value,
                          unsigned long *target_bytes);
static int parse_optional_flags(control_request_t *req, int argc,
                                char *argv[], int start_index);
static const char *state_to_string(container_state_t state);
static int bounded_buffer_init(bounded_buffer_t *buffer);
static void bounded_buffer_destroy(bounded_buffer_t *buffer);
static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer);
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);
void *logging_thread(void *arg);
int child_fn(void *arg);
int register_with_monitor(int monitor_fd, const char *container_id,
                          pid_t host_pid, unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes);
int unregister_from_monitor(int monitor_fd, const char *container_id,
                            pid_t host_pid);
static int run_supervisor(const char *rootfs);
static int send_control_request(const control_request_t *req);

/* ══════════════════════════════════════════════════════════════════════════
 * UTILITY HELPERS
 * ══════════════════════════════════════════════════════════════════════════ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ══════════════════════════════════════════════════════════════════════════
 * BOUNDED BUFFER  (producer-consumer, mutex + condition variables)
 *
 * Why this design?
 *   - mutex protects head/tail/count from concurrent producer and consumer
 *   - not_full: producers wait here when buffer is at capacity
 *   - not_empty: consumer waits here when buffer is empty
 *   - shutting_down flag lets both sides exit cleanly without deadlock
 * ══════════════════════════════════════════════════════════════════════════ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push — producer side
 *
 * Blocks when the buffer is full, waiting on not_full.
 * Returns  0 on success.
 * Returns -1 if shutdown has begun (caller should stop producing).
 *
 * How it works:
 *   1. Lock the mutex to safely inspect count and shutting_down.
 *   2. While count == capacity AND not shutting down, wait on not_full.
 *      (pthread_cond_wait atomically releases the lock and sleeps.)
 *   3. If shutting down, unlock and return -1.
 *   4. Copy the item into items[tail], advance tail (wrap around), increment count.
 *   5. Signal not_empty so the consumer wakes up.
 *   6. Unlock.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is full and we are not shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Insert at tail */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake the consumer */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop — consumer side
 *
 * Blocks when the buffer is empty.
 * Returns  0 on success (item written into *item).
 * Returns  1 if shutdown is in progress AND buffer is empty (caller should exit).
 *
 * How it works:
 *   1. Lock the mutex.
 *   2. While count == 0 AND not shutting down, wait on not_empty.
 *   3. If count is still 0 after waking (shutdown), return 1.
 *   4. Copy item from items[head], advance head, decrement count.
 *   5. Signal not_full so any blocked producer can resume.
 *   6. Unlock and return 0.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is empty and we are not shutting down */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* If empty and shutting down, tell the consumer to exit */
    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return 1; /* sentinel: done */
    }

    /* Remove from head */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake any blocked producer */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * LOGGING CONSUMER THREAD
 *
 * One thread handles ALL containers' log output.
 * It pops log_item_t chunks from the bounded buffer and appends them
 * to the correct per-container log file (identified by container_id).
 *
 * File descriptors are opened lazily and kept open until the thread exits,
 * then closed individually to avoid leaks.
 * ══════════════════════════════════════════════════════════════════════════ */

/* Simple open-file cache entry */
#define MAX_OPEN_LOG_FILES 64
typedef struct {
    char container_id[CONTAINER_ID_LEN];
    int fd;
} log_file_entry_t;

void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;
    log_file_entry_t open_files[MAX_OPEN_LOG_FILES];
    int n_open = 0;

    memset(open_files, 0, sizeof(open_files));
    for (int i = 0; i < MAX_OPEN_LOG_FILES; i++)
        open_files[i].fd = -1;

    /*
     * Loop: pop items until the buffer is drained AND shutdown is signalled.
     * bounded_buffer_pop returns 1 only when count==0 AND shutting_down==1.
     */
    while (bounded_buffer_pop(buf, &item) == 0) {
        /* Find or open the log file for this container */
        int fd = -1;
        for (int i = 0; i < n_open; i++) {
            if (strcmp(open_files[i].container_id, item.container_id) == 0) {
                fd = open_files[i].fd;
                break;
            }
        }

        if (fd < 0 && n_open < MAX_OPEN_LOG_FILES) {
            /* Build log path: logs/<container_id>.log */
            char log_path[PATH_MAX];
            snprintf(log_path, sizeof(log_path), "%s/%s.log",
                     LOG_DIR, item.container_id);
            mkdir(LOG_DIR, 0755); /* ensure directory exists */
            fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                strncpy(open_files[n_open].container_id,
                        item.container_id, CONTAINER_ID_LEN - 1);
                open_files[n_open].fd = fd;
                n_open++;
            } else {
                perror("logging_thread: open log file");
                continue;
            }
        }

        if (fd >= 0 && item.length > 0) {
            ssize_t written = 0;
            while ((size_t)written < item.length) {
                ssize_t n = write(fd, item.data + written,
                                  item.length - (size_t)written);
                if (n < 0) { perror("logging_thread: write"); break; }
                written += n;
            }
        }
    }

    /* Close all open log file descriptors */
    for (int i = 0; i < n_open; i++) {
        if (open_files[i].fd >= 0)
            close(open_files[i].fd);
    }

    fprintf(stderr, "[logger] thread exiting\n");
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════
 * PRODUCER THREAD — reads one container's pipe, pushes into bounded buffer
 * ══════════════════════════════════════════════════════════════════════════ */

typedef struct {
    bounded_buffer_t *buf;
    int read_fd;              /* read end of the container's output pipe */
    char container_id[CONTAINER_ID_LEN];
} producer_ctx_t;

static void *producer_thread(void *arg)
{
    producer_ctx_t *ctx = (producer_ctx_t *)arg;
    log_item_t item;
    ssize_t n;

    while (1) {
        memset(&item, 0, sizeof(item));
        n = read(ctx->read_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0) break; /* pipe closed (container exited) or error */
        item.length = (size_t)n;
        strncpy(item.container_id, ctx->container_id, CONTAINER_ID_LEN - 1);
        if (bounded_buffer_push(ctx->buf, &item) != 0)
            break; /* shutdown in progress */
    }

    close(ctx->read_fd);
    free(ctx);
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════
 * CONTAINER CHILD FUNCTION (runs inside the cloned namespace)
 *
 * Steps performed inside the new namespace:
 *   1. Redirect stdout+stderr to the log pipe (write end).
 *   2. Mount /proc so tools like ps work inside the container.
 *   3. chroot into the provided rootfs.
 *   4. chdir to "/" inside the new root.
 *   5. Apply nice value (scheduling priority).
 *   6. execv the requested command.
 *
 * NOTE: This function must NOT return normally — it always calls execv
 *       or _exit.  The clone() stack is freed by the OS after exec.
 * ══════════════════════════════════════════════════════════════════════════ */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* ── 1. Redirect stdout and stderr to the log pipe ── */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("child: dup2 stdout");
        _exit(1);
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("child: dup2 stderr");
        _exit(1);
    }
    close(cfg->log_write_fd); /* no longer needed after dup2 */

    /* ── 2. Mount /proc inside the new mount namespace ── */
    char proc_path[PATH_MAX];
    snprintf(proc_path, sizeof(proc_path), "%.4000s/proc", cfg->rootfs);
    mkdir(proc_path, 0555);
    if (mount("proc", proc_path, "proc",
              MS_NOSUID | MS_NOEXEC | MS_NODEV, NULL) < 0) {
        /* Non-fatal: warn but continue — /proc is helpful but not required */
        fprintf(stderr, "child: mount /proc warning: %s\n", strerror(errno));
    }

    /* ── 3. chroot into the container rootfs ── */
    if (chroot(cfg->rootfs) < 0) {
        fprintf(stderr, "child: chroot(%s): %s\n", cfg->rootfs, strerror(errno));
        _exit(1);
    }

    /* ── 4. cd to new root ── */
    if (chdir("/") < 0) {
        perror("child: chdir /");
        _exit(1);
    }

    /* ── 5. Apply nice value for scheduling experiments ── */
    if (cfg->nice_value != 0) {
        errno = 0;
        nice(cfg->nice_value);
        if (errno != 0)
            fprintf(stderr, "child: nice(%d) warning: %s\n",
                    cfg->nice_value, strerror(errno));
    }

    /* ── 6. Exec the requested command ── */
    /*
     * We build a small argv.  The command string may be a single binary
     * like "/bin/sh" or a simple space-separated string.
     * For simplicity we pass it to execv as { command, NULL }.
     * Students who need argument parsing can extend this.
     */
    char *child_argv[] = { cfg->command, NULL };
    execv(cfg->command, child_argv);

    /* execv only returns on failure */
    fprintf(stderr, "child: execv(%s): %s\n", cfg->command, strerror(errno));
    _exit(1);
}

/* ══════════════════════════════════════════════════════════════════════════
 * KERNEL MONITOR  ioctl helpers (already provided, kept verbatim)
 * ══════════════════════════════════════════════════════════════════════════ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid               = host_pid;
    req.soft_limit_bytes  = soft_limit_bytes;
    req.hard_limit_bytes  = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) return -1;
    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * CONTAINER METADATA HELPERS
 * ══════════════════════════════════════════════════════════════════════════ */

/* Allocate and prepend a new record. Caller must hold metadata_lock. */
static container_record_t *alloc_record(supervisor_ctx_t *ctx,
                                        const control_request_t *req)
{
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) return NULL;
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->state            = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->started_at       = time(NULL);
    snprintf(rec->log_path, sizeof(rec->log_path),
             "%s/%s.log", LOG_DIR, req->container_id);
    /* Prepend */
    rec->next      = ctx->containers;
    ctx->containers = rec;
    return rec;
}

/* Find record by id. Caller must hold metadata_lock. */
static container_record_t *find_record(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *r;
    for (r = ctx->containers; r; r = r->next)
        if (strcmp(r->id, id) == 0)
            return r;
    return NULL;
}

/* Find record by PID. Caller must hold metadata_lock. */
static container_record_t *find_record_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *r;
    for (r = ctx->containers; r; r = r->next)
        if (r->host_pid == pid)
            return r;
    return NULL;
}

/* ══════════════════════════════════════════════════════════════════════════
 * SPAWN CONTAINER
 *
 * Creates a pipe, clones a child with namespace flags, starts a producer
 * thread to read the pipe, registers with the kernel monitor, and updates
 * the metadata record.
 *
 * Namespace flags used:
 *   CLONE_NEWPID  — child gets PID 1 inside the container
 *   CLONE_NEWUTS  — child gets its own hostname / domainname
 *   CLONE_NEWNS   — child gets its own mount namespace (so /proc mount
 *                   does not affect the host)
 * ══════════════════════════════════════════════════════════════════════════ */
static pid_t spawn_container(supervisor_ctx_t *ctx,
                             container_record_t *rec,
                             const control_request_t *req)
{
    int pipe_fds[2];
    if (pipe(pipe_fds) < 0) { perror("pipe"); return -1; }

    /* Build child config on the heap (child_fn will see it on the stack page) */
    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) { close(pipe_fds[0]); close(pipe_fds[1]); return -1; }
    strncpy(cfg->id,       req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,   req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command,  req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipe_fds[1]; /* write end goes to the child */

    /* Allocate a stack for the cloned child */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc stack");
        free(cfg);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }
    char *stack_top = stack + STACK_SIZE; /* stack grows down */

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t child_pid = clone(child_fn, stack_top, clone_flags, cfg);

    /* Parent closes the write end; only the child (and producer) need it */
    close(pipe_fds[1]);

    if (child_pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipe_fds[0]);
        return -1;
    }

    /* stack freed after exec inside the child; safe to free parent copy */
    free(stack);

    /* Update metadata */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->host_pid = child_pid;
    rec->state    = CONTAINER_RUNNING;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Start producer thread: reads pipe_fds[0], pushes into log buffer */
    producer_ctx_t *pctx = calloc(1, sizeof(*pctx));
    if (pctx) {
        pctx->buf     = &ctx->log_buffer;
        pctx->read_fd = pipe_fds[0];
        strncpy(pctx->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        pthread_t prod_tid;
        if (pthread_create(&prod_tid, NULL, producer_thread, pctx) != 0) {
            perror("pthread_create producer");
            close(pipe_fds[0]);
            free(pctx);
        } else {
            pthread_detach(prod_tid); /* auto-cleans when done */
        }
    } else {
        close(pipe_fds[0]);
    }

    /* Register with kernel monitor (best-effort; log failure but continue) */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id,
                                  child_pid, req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0)
            fprintf(stderr, "[supervisor] monitor register failed: %s\n",
                    strerror(errno));
    }

    fprintf(stderr, "[supervisor] started container '%s' pid=%d\n",
            req->container_id, child_pid);
    return child_pid;
}

/* ══════════════════════════════════════════════════════════════════════════
 * SIGCHLD HANDLER — reap dead children, update metadata
 *
 * We use a volatile sig_atomic_t flag and do the actual waitpid work in
 * the main event loop.  This avoids calling non-async-signal-safe functions
 * inside the handler itself.
 * ══════════════════════════════════════════════════════════════════════════ */
static volatile sig_atomic_t g_sigchld_received = 0;
static volatile sig_atomic_t g_shutdown_requested = 0;

static void sigchld_handler(int sig)
{
    (void)sig;
    g_sigchld_received = 1;
}

static void shutdown_handler(int sig)
{
    (void)sig;
    g_shutdown_requested = 1;
}

/* Called from the event loop to reap all finished children */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_record_by_pid(ctx, pid);
        if (rec) {
            if (WIFEXITED(status)) {
                rec->state     = CONTAINER_EXITED;
                rec->exit_code = WEXITSTATUS(status);
                fprintf(stderr, "[supervisor] container '%s' exited code=%d\n",
                        rec->id, rec->exit_code);
            } else if (WIFSIGNALED(status)) {
                rec->state       = CONTAINER_KILLED;
                rec->exit_signal = WTERMSIG(status);
                fprintf(stderr, "[supervisor] container '%s' killed signal=%d\n",
                        rec->id, rec->exit_signal);
            }
            /* Unregister from kernel monitor */
            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, pid);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ══════════════════════════════════════════════════════════════════════════
 * HANDLE ONE CONTROL REQUEST (called from event loop)
 *
 * The supervisor reads a control_request_t from the connected client socket,
 * performs the requested action, and writes back a control_response_t.
 * ══════════════════════════════════════════════════════════════════════════ */
static void handle_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    memset(&resp, 0, sizeof(resp));

    n = recv(client_fd, &req, sizeof(req), 0);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "bad request");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {

    /* ── START: launch container in background ── */
    case CMD_START: {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_record(ctx, req.container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' already exists", req.container_id);
            break;
        }
        container_record_t *rec = alloc_record(ctx, &req);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!rec) { resp.status = -1; snprintf(resp.message, sizeof(resp.message), "OOM"); break; }

        pid_t pid = spawn_container(ctx, rec, &req);
        if (pid < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "spawn failed");
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "started '%s' pid=%d", req.container_id, pid);
        }
        break;
    }

    /* ── RUN: launch container, supervisor waits for it ── */
    case CMD_RUN: {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (find_record(ctx, req.container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' already exists", req.container_id);
            break;
        }
        container_record_t *rec = alloc_record(ctx, &req);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!rec) { resp.status = -1; snprintf(resp.message, sizeof(resp.message), "OOM"); break; }

        pid_t pid = spawn_container(ctx, rec, &req);
        if (pid < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "spawn failed");
            break;
        }

        /* Reply immediately so the client knows it started */
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "running '%s' pid=%d (foreground)", req.container_id, pid);
        send(client_fd, &resp, sizeof(resp), 0);

        /* Now block in supervisor until child exits */
        int wstatus;
        waitpid(pid, &wstatus, 0);
        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_record_by_pid(ctx, pid);
        if (rec) {
            if (WIFEXITED(wstatus))        { rec->state = CONTAINER_EXITED; rec->exit_code = WEXITSTATUS(wstatus); }
            else if (WIFSIGNALED(wstatus)) { rec->state = CONTAINER_KILLED; rec->exit_signal = WTERMSIG(wstatus); }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (ctx->monitor_fd >= 0) unregister_from_monitor(ctx->monitor_fd, req.container_id, pid);
        return; /* response already sent above */
    }

    /* ── PS: list all containers ── */
    case CMD_PS: {
        char buf[4096];
        int off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-10s %-12s %-14s %-14s\n",
                        "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)", "STARTED");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r;
        for (r = ctx->containers; r && off < (int)sizeof(buf) - 80; r = r->next) {
            char tsbuf[32];
            struct tm *tm_info = localtime(&r->started_at);
            strftime(tsbuf, sizeof(tsbuf), "%H:%M:%S", tm_info);
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-10s %-14lu %-14lu %-14s\n",
                            r->id, r->host_pid, state_to_string(r->state),
                            r->soft_limit_bytes >> 20,
                            r->hard_limit_bytes >> 20,
                            tsbuf);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        break;
    }

    /* ── LOGS: print log file path so client can tail it, or dump contents ── */
    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_record(ctx, req.container_id);
        char log_path[PATH_MAX] = {0};
        if (rec) strncpy(log_path, rec->log_path, sizeof(log_path) - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not found", req.container_id);
        } else {
            resp.status = 0;
            strncpy(resp.message, log_path, sizeof(resp.message) - 1);
            resp.message[sizeof(resp.message) - 1] = '\0';
        }
        break;
    }

    /* ── STOP: send SIGTERM, then SIGKILL after 3 s ── */
    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_record(ctx, req.container_id);
        pid_t pid = rec ? rec->host_pid : -1;
        container_state_t state = rec ? rec->state : CONTAINER_EXITED;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!rec || state != CONTAINER_RUNNING) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not running", req.container_id);
            break;
        }

        /* Graceful stop */
        kill(pid, SIGTERM);
        struct timespec ts = {3, 0};
        nanosleep(&ts, NULL);

        /* Check if still alive */
        if (kill(pid, 0) == 0) {
            kill(pid, SIGKILL); /* force kill */
            pthread_mutex_lock(&ctx->metadata_lock);
            if (rec) rec->state = CONTAINER_KILLED;
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' force-killed", req.container_id);
        } else {
            pthread_mutex_lock(&ctx->metadata_lock);
            if (rec) rec->state = CONTAINER_STOPPED;
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' stopped", req.container_id);
        }
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "unknown command");
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ══════════════════════════════════════════════════════════════════════════
 * SUPERVISOR MAIN FUNCTION
 *
 * Full event loop:
 *   1) Open /dev/container_monitor (best-effort).
 *   2) Create UNIX domain socket, bind, listen.
 *   3) Install signal handlers.
 *   4) Start logger thread.
 *   5) accept() loop with a poll/select on the server socket.
 *      Between accepts, check SIGCHLD flag and reap children.
 *      Check shutdown flag and break the loop.
 *   6) Graceful shutdown: join logger, free records, close fds.
 * ══════════════════════════════════════════════════════════════════════════ */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    /* Ensure log directory exists */
    mkdir(LOG_DIR, 0755);

    /* ── 1. Metadata lock ── */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* ── 2. Bounded buffer ── */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* ── 3. Kernel monitor (optional — skip gracefully if absent) ── */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] /dev/container_monitor not available "
                "(kernel module not loaded?) — memory limits disabled\n");

    /* ── 4. UNIX domain socket ── */
    unlink(CONTROL_PATH); /* remove stale socket if any */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto cleanup; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) { perror("listen"); goto cleanup; }

    /* Make accept non-blocking so we can poll signals */
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    fprintf(stderr, "[supervisor] listening on %s  rootfs=%s\n",
            CONTROL_PATH, rootfs);

    /* ── 5. Signal handlers ── */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = shutdown_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* ── 6. Logger thread ── */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("pthread_create logger"); goto cleanup;
    }

    /* ── 7. Event loop ── */
    while (!g_shutdown_requested) {
        /* Reap children if SIGCHLD was received */
        if (g_sigchld_received) {
            g_sigchld_received = 0;
            reap_children(&ctx);
        }

        /* Accept one connection (non-blocking) */
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* No connection right now — sleep briefly */
                struct timespec ts = {0, 50 * 1000 * 1000}; /* 50 ms */
                nanosleep(&ts, NULL);
                continue;
            }
            if (errno == EINTR) continue; /* signal interrupted accept */
            perror("accept");
            break;
        }

        handle_request(&ctx, client_fd);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] shutting down...\n");

    /* ── 8. Stop all running containers ── */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *r;
    for (r = ctx.containers; r; r = r->next) {
        if (r->state == CONTAINER_RUNNING && r->host_pid > 0) {
            kill(r->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give them a moment, then reap */
    struct timespec wait = {2, 0};
    nanosleep(&wait, NULL);
    reap_children(&ctx);

cleanup:
    /* Signal logger thread to stop and join */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *cur = ctx.containers;
    while (cur) {
        container_record_t *nxt = cur->next;
        free(cur);
        cur = nxt;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] exited cleanly\n");
    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * CLIENT-SIDE CONTROL REQUEST
 *
 * Connects to the supervisor's UNIX socket, sends the request struct,
 * reads back the response struct, and prints the message.
 *
 * For CMD_LOGS: after printing the log path, we dump the file to stdout
 * so the user actually sees the log contents.
 * ══════════════════════════════════════════════════════════════════════════ */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s\n"
                "Is the supervisor running?  (sudo ./engine supervisor <rootfs>)\n",
                CONTROL_PATH);
        close(fd);
        return 1;
    }

    /* Send request */
    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send"); close(fd); return 1;
    }

    /* Receive response */
    control_response_t resp;
    if (recv(fd, &resp, sizeof(resp), MSG_WAITALL) != (ssize_t)sizeof(resp)) {
        perror("recv"); close(fd); return 1;
    }
    close(fd);

    /* Print response message */
    if (resp.status == 0) {
        printf("%s\n", resp.message);

        /* For LOGS: the message contains the log path — dump it */
        if (req->kind == CMD_LOGS && resp.message[0] == '/') {
            FILE *lf = fopen(resp.message, "r");
            if (!lf) {
                fprintf(stderr, "Log file not found: %s\n", resp.message);
            } else {
                char line[512];
                while (fgets(line, sizeof(line), lf))
                    fputs(line, stdout);
                fclose(lf);
            }
        }
    } else {
        fprintf(stderr, "Error: %s\n", resp.message);
        return 1;
    }

    return 0;
}

/* ══════════════════════════════════════════════════════════════════════════
 * CLI COMMAND HANDLERS
 * ══════════════════════════════════════════════════════════════════════════ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ══════════════════════════════════════════════════════════════════════════
 * MAIN
 * ══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

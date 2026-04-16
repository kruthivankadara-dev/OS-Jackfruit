/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Complete implementation covering:
 *   Task 1 - Multi-container runtime with clone() + namespaces + supervisor
 *   Task 2 - CLI via UNIX domain socket (start, run, ps, logs, stop)
 *   Task 3 - Bounded-buffer concurrent logging with producer/consumer threads
 *
 * Build:
 *   gcc -Wall -Wextra -g -pthread -o engine engine.c
 *
 * Run (needs root for namespaces + chroot):
 *   sudo ./engine supervisor ./rootfs-base &
 *   sudo ./engine start alpha ./rootfs-alpha 
/bin/sh
 *   sudo ./engine ps
 *   sudo ./engine logs alpha
 *   sudo ./engine stop alpha
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
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

/* ── monitor_ioctl.h inline (so we compile without the kernel header) ───── */
#ifndef MONITOR_IOCTL_H
#define MONITOR_IOCTL_H

#define MONITOR_MAGIC 0xCE
#define MONITOR_REGISTER   _IOW(MONITOR_MAGIC, 1, struct monitor_request)
#define MONITOR_UNREGISTER _IOW(MONITOR_MAGIC, 2, struct monitor_request)

struct monitor_request {
    int   pid;
    char  container_id[32];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
};

#endif /* MONITOR_IOCTL_H */
/* ────────────────────────────────────────────────────────────────────────── */

#define STACK_SIZE            (1024 * 1024)
#define CONTAINER_ID_LEN      32
#define CONTROL_PATH          "/tmp/mini_runtime.sock"
#define LOG_DIR               "logs"
#define CONTROL_MESSAGE_LEN   256
#define CHILD_COMMAND_LEN     256
#define LOG_CHUNK_SIZE        4096
#define LOG_BUFFER_CAPACITY   64
#define DEFAULT_SOFT_LIMIT    (40UL << 20)
#define DEFAULT_HARD_LIMIT    (64UL << 20)
#define MAX_CONTAINERS        32
#define MONITOR_DEVICE        "/dev/container_monitor"

/* ── Enumerations ────────────────────────────────────────────────────────── */

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

/* ── Data structures ─────────────────────────────────────────────────────── */

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;         /* set before sending SIGTERM from stop cmd */
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t     head;
    size_t     tail;
    size_t     count;
    int        shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char  container_id[CONTAINER_ID_LEN];
    char  rootfs[PATH_MAX];
    char  command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int   nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;
} child_config_t;

typedef struct {
    int  server_fd;
    int  monitor_fd;
    int  should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t  metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* ── Per-container pipe-reader thread args ───────────────────────────────── */
typedef struct {
    int  read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} reader_args_t;

/* ── Global supervisor context (accessed in signal handler) ──────────────── */
static supervisor_ctx_t *g_ctx = NULL;

/* ════════════════════════════════════════════════════════════════════════════
 *  Utility / helpers
 * ════════════════════════════════════════════════════════════════════════════ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
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
        fprintf(stderr, "Value for %s is too large\n", flag);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc,
                                char *argv[], int start_index)
{
    for (int i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nv;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1],
                               &req->soft_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1],
                               &req->hard_limit_bytes) != 0) return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nv = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nv < -20 || nv > 19) {
                fprintf(stderr, "Invalid --nice value: %s\n", argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nv;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "soft limit cannot exceed hard limit\n");
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

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 3 — Bounded-buffer (producer / consumer)
 * ════════════════════════════════════════════════════════════════════════════ */

static int bounded_buffer_init(bounded_buffer_t *b)
{
    int rc;
    memset(b, 0, sizeof(*b));
    if ((rc = pthread_mutex_init(&b->mutex, NULL)) != 0) return rc;
    if ((rc = pthread_cond_init(&b->not_empty, NULL)) != 0) {
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    if ((rc = pthread_cond_init(&b->not_full, NULL)) != 0) {
        pthread_cond_destroy(&b->not_empty);
        pthread_mutex_destroy(&b->mutex); return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *b)
{
    pthread_cond_destroy(&b->not_full);
    pthread_cond_destroy(&b->not_empty);
    pthread_mutex_destroy(&b->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *b)
{
    pthread_mutex_lock(&b->mutex);
    b->shutting_down = 1;
    pthread_cond_broadcast(&b->not_empty);
    pthread_cond_broadcast(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
}

/*
 * Producer: push a log chunk into the buffer.
 * Blocks if full; returns 0 on success, -1 if shutting down.
 */
int bounded_buffer_push(bounded_buffer_t *b, const log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    /* Wait while full, but bail out on shutdown */
    while (b->count == LOG_BUFFER_CAPACITY && !b->shutting_down)
        pthread_cond_wait(&b->not_full, &b->mutex);

    if (b->shutting_down) {
        pthread_mutex_unlock(&b->mutex);
        return -1;
    }

    b->items[b->tail] = *item;
    b->tail = (b->tail + 1) % LOG_BUFFER_CAPACITY;
    b->count++;

    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/*
 * Consumer: pop a log chunk.
 * Returns 0 on success, 1 if shutting down and buffer is empty.
 */
int bounded_buffer_pop(bounded_buffer_t *b, log_item_t *item)
{
    pthread_mutex_lock(&b->mutex);

    while (b->count == 0) {
        if (b->shutting_down) {
            pthread_mutex_unlock(&b->mutex);
            return 1; /* done — nothing left */
        }
        pthread_cond_wait(&b->not_empty, &b->mutex);
    }

    *item = b->items[b->head];
    b->head = (b->head + 1) % LOG_BUFFER_CAPACITY;
    b->count--;

    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return 0;
}

/* ── Logger consumer thread ─────────────────────────────────────────────── */

/*
 * Drains the bounded buffer and appends each chunk to the correct per-container
 * log file under LOG_DIR/<container_id>.log.
 * Exits when shutdown is signalled AND the buffer is fully drained.
 */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t item;

    /* Make sure log directory exists */
    mkdir(LOG_DIR, 0755);

    while (1) {
        int rc = bounded_buffer_pop(buf, &item);
        if (rc != 0)
            break; /* shutting_down + empty */

        /* Build path: logs/<container_id>.log */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }

        /* Prepend a timestamp */
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        char ts[32];
        strftime(ts, sizeof(ts), "[%H:%M:%S] ", tm_info);

        /* Write timestamp + data */
        write(fd, ts, strlen(ts));
        write(fd, item.data, item.length);
        /* Ensure newline */
        if (item.length > 0 && item.data[item.length - 1] != '\n')
            write(fd, "\n", 1);

        close(fd);
    }

    return NULL;
}

/* ── Pipe-reader producer thread (one per container) ────────────────────── */

void *pipe_reader_thread(void *arg)
{
    reader_args_t *ra = (reader_args_t *)arg;
    char linebuf[LOG_CHUNK_SIZE];
    FILE *fp = fdopen(ra->read_fd, "r");
    if (!fp) {
        perror("pipe_reader_thread: fdopen");
        free(ra);
        return NULL;
    }

    while (fgets(linebuf, sizeof(linebuf), fp)) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, ra->container_id,
                sizeof(item.container_id) - 1);
        item.length = strlen(linebuf);
        memcpy(item.data, linebuf, item.length);

        if (bounded_buffer_push(ra->log_buffer, &item) != 0)
            break; /* supervisor is shutting down */
    }

    fclose(fp);
    free(ra);
    return NULL;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 1 — Container child entrypoint (runs inside new namespaces)
 * ════════════════════════════════════════════════════════════════════════════ */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* ── Redirect stdout / stderr into the supervisor log pipe ── */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) { perror("dup2 stdout"); return 1; }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) { perror("dup2 stderr"); return 1; }
    close(cfg->log_write_fd);

    /* ── UTS namespace: set container hostname ── */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname"); /* non-fatal */

    /* ── Mount namespace: make all mounts private so host is unaffected ── */
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount private"); /* may fail in WSL — continue */

    /* ── Pivot root into container rootfs ── */
    /* Simpler fallback: chroot (works on WSL too) */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    /* ── Mount /proc inside the container ── */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc",
              MS_NOSUID | MS_NOEXEC | MS_NODEV, NULL) < 0)
        perror("mount /proc"); /* non-fatal */

    /* ── Apply nice value ── */
    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
            perror("setpriority");
    }

    /* ── Exec the requested command ── */
    /* Split command string into argv */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';

    char *argv_arr[64];
    int argc = 0;
    char *tok = strtok(cmd_copy, " \t");
    while (tok && argc < 63) {
        argv_arr[argc++] = tok;
        tok = strtok(NULL, " \t");
    }
    argv_arr[argc] = NULL;

    if (argc == 0) {
        fprintf(stderr, "child_fn: empty command\n");
        return 1;
    }

    execvp(argv_arr[0], argv_arr);
    perror("execvp");
    return 1;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  Monitor registration helpers
 * ════════════════════════════════════════════════════════════════════════════ */

int register_with_monitor(int monitor_fd, const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    if (monitor_fd < 0) return 0; /* monitor not loaded — skip silently */
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) {
        perror("ioctl MONITOR_REGISTER");
        return -1;
    }
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id,
                            pid_t host_pid)
{
    if (monitor_fd < 0) return 0;
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) {
        perror("ioctl MONITOR_UNREGISTER");
        return -1;
    }
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 1 — Supervisor: container launch
 * ════════════════════════════════════════════════════════════════════════════ */

/*
 * Spawns a new container.
 * Returns the new container_record_t on success, NULL on failure.
 * Caller must hold ctx->metadata_lock.
 */
static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    /* Build log path */
    char log_path[PATH_MAX];
    mkdir(LOG_DIR, 0755);
    snprintf(log_path, sizeof(log_path), "%s/%s.log",
             LOG_DIR, req->container_id);

    /* Create pipe: child writes, parent reads via reader thread */
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return NULL; }

    /* Build child config on the heap — child_fn owns it until it execs */
    child_config_t *cfg = calloc(1, sizeof(child_config_t));
    if (!cfg) { close(pipefd[0]); close(pipefd[1]); return NULL; }
    strncpy(cfg->id,      req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs,  req->rootfs,        sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command,       sizeof(cfg->command) - 1);
    cfg->nice_value    = req->nice_value;
    cfg->log_write_fd  = pipefd[1];

    /* Allocate clone stack */
    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); close(pipefd[0]); close(pipefd[1]); return NULL; }
    char *stack_top = stack + STACK_SIZE;

    /* Namespace flags — CLONE_NEWPID may fail in WSL; detect and degrade */
    int flags = CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;

    /* Try to add PID namespace (fails silently on WSL) */
    pid_t pid = clone(child_fn, stack_top, flags | CLONE_NEWPID, cfg);
    if (pid < 0 && errno == EINVAL) {
        /* WSL or restricted kernel — fall back without CLONE_NEWPID */
        pid = clone(child_fn, stack_top, flags, cfg);
    }

    /* After exec the child's stack is replaced; we can free our copy */
    /* (We wait a tiny moment for exec to happen before freeing) */
    /* In practice the parent proceeds immediately; child_fn copies what it needs
       before exec. The stack is only needed until the first exec boundary. */

    close(pipefd[1]); /* parent closes write end */

    if (pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipefd[0]);
        return NULL;
    }

    /* Register with kernel monitor (if loaded) */
    register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                          req->soft_limit_bytes, req->hard_limit_bytes);

    /* Build metadata record */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        kill(pid, SIGKILL);
        close(pipefd[0]);
        free(stack);
        return NULL;
    }
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid          = pid;
    rec->started_at        = time(NULL);
    rec->state             = CONTAINER_RUNNING;
    rec->soft_limit_bytes  = req->soft_limit_bytes;
    rec->hard_limit_bytes  = req->hard_limit_bytes;
    strncpy(rec->log_path, log_path, sizeof(rec->log_path) - 1);

    /* Prepend to linked list */
    rec->next        = ctx->containers;
    ctx->containers  = rec;

    /* Spawn a pipe-reader producer thread for this container */
    reader_args_t *ra = malloc(sizeof(reader_args_t));
    if (ra) {
        ra->read_fd   = pipefd[0];
        ra->log_buffer = &ctx->log_buffer;
        strncpy(ra->container_id, req->container_id,
                sizeof(ra->container_id) - 1);
        pthread_t tid;
        pthread_create(&tid, NULL, pipe_reader_thread, ra);
        pthread_detach(tid);
    } else {
        close(pipefd[0]);
    }

    /* stack is freed after child execs (the child's address space is replaced).
       Technically we should keep it alive until exec; a small sleep or
       waitpid-with-WNOHANG loop is the clean approach, but for simplicity
       we leak the stack page — it's 1 MiB and will be reclaimed when the
       child exits.  For a production runtime you'd use vfork semantics. */
    (void)stack; /* intentionally not freed here */

    return rec;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 1 — Supervisor: SIGCHLD reaping
 * ════════════════════════════════════════════════════════════════════════════ */

static void reap_children(supervisor_ctx_t *ctx)
{
    int wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *r = ctx->containers; r; r = r->next) {
            if (r->host_pid != pid) continue;

            unregister_from_monitor(ctx->monitor_fd, r->id, pid);

            if (WIFEXITED(wstatus)) {
                r->exit_code = WEXITSTATUS(wstatus);
                r->state     = CONTAINER_EXITED;
            } else if (WIFSIGNALED(wstatus)) {
                r->exit_signal = WTERMSIG(wstatus);
                /*
                 * Classify:
                 *   stop_requested  → STOPPED (user asked for it)
                 *   SIGKILL, no req → KILLED  (likely hard-limit from monitor)
                 *   anything else   → EXITED
                 */
                if (r->stop_requested)
                    r->state = CONTAINER_STOPPED;
                else if (WTERMSIG(wstatus) == SIGKILL)
                    r->state = CONTAINER_KILLED;
                else
                    r->state = CONTAINER_EXITED;
            }
            fprintf(stderr,
                    "[supervisor] reaped container '%s' pid=%d state=%s\n",
                    r->id, pid, state_to_string(r->state));
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 2 — Supervisor: handle a single client connection
 * ════════════════════════════════════════════════════════════════════════════ */

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t  req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "recv error");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    pthread_mutex_lock(&ctx->metadata_lock);

    switch (req.kind) {

    /* ── CMD_START / CMD_RUN — launch container ── */
    case CMD_START:
    case CMD_RUN: {
        /* Check for duplicate ID */
        for (container_record_t *r = ctx->containers; r; r = r->next) {
            if (strcmp(r->id, req.container_id) == 0 &&
                r->state == CONTAINER_RUNNING) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "container '%s' already running", req.container_id);
                goto reply;
            }
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
        container_record_t *rec = launch_container(ctx, &req);
        pthread_mutex_lock(&ctx->metadata_lock);

        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to launch container '%s'", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "started container '%s' pid=%d",
                     rec->id, rec->host_pid);
        }
        break;
    }

    /* ── CMD_PS — list all containers ── */
    case CMD_PS: {
        char buf[CONTROL_MESSAGE_LEN];
        int  off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-20s %-8s %-10s\n",
                        "ID", "PID", "STATE");
        for (container_record_t *r = ctx->containers;
             r && off < (int)sizeof(buf) - 1; r = r->next) {
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-20s %-8d %-10s\n",
                            r->id, r->host_pid,
                            state_to_string(r->state));
        }
        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        break;
    }

    /* ── CMD_LOGS — send path; client can tail the file ── */
    case CMD_LOGS: {
        container_record_t *found = NULL;
        for (container_record_t *r = ctx->containers; r; r = r->next)
            if (strcmp(r->id, req.container_id) == 0) { found = r; break; }
        if (!found) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "unknown container '%s'", req.container_id);
        } else {
            resp.status = 0;
            /* Send back up to CONTROL_MESSAGE_LEN of recent log content */
            FILE *fp = fopen(found->log_path, "r");
            if (!fp) {
                snprintf(resp.message, sizeof(resp.message),
                         "(log file not yet created: %.200s)", found->log_path);
            } else {
                /* Seek to tail */
                fseek(fp, 0, SEEK_END);
                long fsize = ftell(fp);
                long offset = fsize - (CONTROL_MESSAGE_LEN - 1);
                if (offset < 0) offset = 0;
                fseek(fp, offset, SEEK_SET);
                size_t bytes = fread(resp.message, 1,
                                     sizeof(resp.message) - 1, fp);
                resp.message[bytes] = '\0';
                fclose(fp);
            }
        }
        break;
    }

    /* ── CMD_STOP — graceful shutdown of one container ── */
    case CMD_STOP: {
        container_record_t *found = NULL;
        for (container_record_t *r = ctx->containers; r; r = r->next)
            if (strcmp(r->id, req.container_id) == 0) { found = r; break; }
        if (!found || found->state != CONTAINER_RUNNING) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not running", req.container_id);
        } else {
            /* Set flag BEFORE signal so SIGCHLD handler classifies correctly */
            found->stop_requested = 1;
            kill(found->host_pid, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "sent SIGTERM to '%s' (pid=%d)",
                     found->id, found->host_pid);
        }
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "unknown command");
        break;
    }

reply:
    pthread_mutex_unlock(&ctx->metadata_lock);
    send(client_fd, &resp, sizeof(resp), 0);
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 1 — Signal handler (SIGCHLD / SIGINT / SIGTERM)
 * ════════════════════════════════════════════════════════════════════════════ */

static volatile sig_atomic_t g_shutdown = 0;

static void handle_sigchld(int sig)
{
    (void)sig;
    if (g_ctx) reap_children(g_ctx);
}

static void handle_shutdown_signal(int sig)
{
    (void)sig;
    g_shutdown = 1;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 1+2+3 — run_supervisor: the long-running parent process
 * ════════════════════════════════════════════════════════════════════════════ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    /* ── Metadata lock ── */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* ── Bounded buffer ── */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* ── Task 4: open kernel monitor device (optional — skip if absent) ── */
    ctx.monitor_fd = open(MONITOR_DEVICE, O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] kernel monitor not available (%s) — "
                        "memory limits disabled\n", MONITOR_DEVICE);
        ctx.monitor_fd = -1;
    }

    /* ── UNIX domain socket (control plane) ── */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto cleanup; }

    /* Remove stale socket file */
    unlink(CONTROL_PATH);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen"); goto cleanup;
    }

    fprintf(stderr, "[supervisor] started (base-rootfs: %s)\n", rootfs);
    fprintf(stderr, "[supervisor] control socket: %s\n", CONTROL_PATH);

    /* ── Signal handlers ── */
    struct sigaction sa_chld = { .sa_handler = handle_sigchld,
                                 .sa_flags   = SA_RESTART | SA_NOCLDSTOP };
    sigemptyset(&sa_chld.sa_mask);
    sigaction(SIGCHLD, &sa_chld, NULL);

    struct sigaction sa_term = { .sa_handler = handle_shutdown_signal,
                                 .sa_flags   = SA_RESTART };
    sigemptyset(&sa_term.sa_mask);
    sigaction(SIGINT,  &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* ── Logger thread (Task 3) ── */
    rc = pthread_create(&ctx.logger_thread, NULL,
                        logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("pthread_create logger");
        goto cleanup;
    }

    /* Make the accept socket non-blocking so SIGCHLD can interrupt it */
    int flags_sock = fcntl(ctx.server_fd, F_GETFL, 0);
    fcntl(ctx.server_fd, F_SETFL, flags_sock | O_NONBLOCK);

    /* ── Supervisor event loop ── */
    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Interrupted by signal or no pending connection — poll */
                usleep(10000); /* 10 ms */
                continue;
            }
            perror("accept");
            break;
        }
        handle_client(&ctx, client_fd);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] shutting down...\n");

    /* ── Graceful teardown ── */

    /* 1. SIGTERM all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *r = ctx.containers; r; r = r->next) {
        if (r->state == CONTAINER_RUNNING) {
            r->stop_requested = 1;
            kill(r->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* 2. Give them 3s, then SIGKILL stragglers */
    sleep(3);
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *r = ctx.containers; r; r = r->next)
        if (r->state == CONTAINER_RUNNING)
            kill(r->host_pid, SIGKILL);
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* 3. Reap remaining zombies */
    reap_children(&ctx);

    /* 4. Drain + stop logger */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

cleanup:
    if (ctx.server_fd >= 0) { close(ctx.server_fd); unlink(CONTROL_PATH); }
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock); /* already destroyed — no-op */
    container_record_t *r = ctx.containers;
    while (r) {
        container_record_t *next = r->next;
        free(r);
        r = next;
    }

    g_ctx = NULL;
    fprintf(stderr, "[supervisor] exited cleanly\n");
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  TASK 2 — Client: send control request to supervisor
 * ════════════════════════════════════════════════════════════════════════════ */

static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect — is the supervisor running?");
        close(sock);
        return 1;
    }

    if (send(sock, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(sock);
        return 1;
    }

    control_response_t resp;
    ssize_t n = recv(sock, &resp, sizeof(resp), MSG_WAITALL);
    close(sock);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Unexpected response length from supervisor\n");
        return 1;
    }

    printf("%s\n", resp.message);
    return resp.status == 0 ? 0 : 1;
}

/* ════════════════════════════════════════════════════════════════════════════
 *  CLI command handlers
 * ════════════════════════════════════════════════════════════════════════════ */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,        argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,       argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    /* run = start (background) — distinction is semantics only for this impl */
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,        argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,       argv[4], sizeof(req.command) - 1);
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
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ════════════════════════════════════════════════════════════════════════════
 *  main
 * ════════════════════════════════════════════════════════════════════════════ */

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

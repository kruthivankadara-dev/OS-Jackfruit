#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/jiffies.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ================= TODO 1 ================= */
struct monitored_entry {
    pid_t pid;
    char container_id[64];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    bool soft_warned;
    struct list_head list;
};

/* ================= TODO 2 ================= */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitor_lock);

/* Provided */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ================= RSS ================= */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ================= HELPERS ================= */
static void log_soft_limit_event(const char *id, pid_t pid,
                                unsigned long limit, long rss)
{
    printk(KERN_WARNING
           "[monitor] SOFT LIMIT %s pid=%d rss=%ld limit=%lu\n",
           id, pid, rss, limit);
}

static void kill_process(const char *id, pid_t pid,
                         unsigned long limit, long rss)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[monitor] HARD LIMIT %s pid=%d rss=%ld limit=%lu\n",
           id, pid, rss, limit);
}

/* ================= TODO 3 ================= */
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;

    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {

        long rss = get_rss_bytes(entry->pid);

        if (rss < 0) {
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if (entry->hard_limit_bytes > 0 &&
            rss > entry->hard_limit_bytes) {

            kill_process(entry->container_id,
                         entry->pid,
                         entry->hard_limit_bytes,
                         rss);

            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        if (entry->soft_limit_bytes > 0 &&
            rss > entry->soft_limit_bytes &&
            !entry->soft_warned) {

            log_soft_limit_event(entry->container_id,
                                 entry->pid,
                                 entry->soft_limit_bytes,
                                 rss);

            entry->soft_warned = true;
        }
    }

    mutex_unlock(&monitor_lock);

    mod_timer(&monitor_timer,
              jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ================= TODO 4 & 5 ================= */
static long monitor_ioctl(struct file *f,
                          unsigned int cmd,
                          unsigned long arg)
{
    struct monitor_request req;

    if (copy_from_user(&req,
        (struct monitor_request __user *)arg,
        sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {

        struct monitored_entry *entry;

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = req.pid;

        strncpy(entry->container_id,
                req.container_id,
                sizeof(entry->container_id) - 1);
        entry->container_id[63] = '\0';

        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned = false;

        INIT_LIST_HEAD(&entry->list);

        mutex_lock(&monitor_lock);
        list_add_tail(&entry->list, &monitored_list);
        mutex_unlock(&monitor_lock);

        return 0;
    }

    else if (cmd == MONITOR_UNREGISTER) {

        struct monitored_entry *entry, *tmp;
        int found = 0;

        mutex_lock(&monitor_lock);

        list_for_each_entry_safe(entry, tmp,
                                &monitored_list, list) {

            if (entry->pid == req.pid &&
                strncmp(entry->container_id,
                        req.container_id,
                        sizeof(entry->container_id)) == 0) {

                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }

        mutex_unlock(&monitor_lock);

        return found ? 0 : -ENOENT;
    }

    return -EINVAL;
}

/* ================= FILE OPS ================= */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* ================= INIT ================= */
static int __init monitor_init(void)
{
    alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6,4,0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif

    device_create(cl, NULL, dev_num, NULL, DEVICE_NAME);

    cdev_init(&c_dev, &fops);
    cdev_add(&c_dev, dev_num, 1);

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer,
              jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "monitor loaded\n");
    return 0;
}

/* ================= TODO 6 ================= */
static void __exit monitor_exit(void)
{
    struct monitored_entry *entry, *tmp;

    /* stop timer safely */
    timer_delete_sync(&monitor_timer);

    /* free all entries */
    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp,
                            &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    mutex_unlock(&monitor_lock);

    /* remove device */
    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "monitor unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");

# 🚀 OS Jackfruit — Multi-Container Runtime from Scratch in C

> A lightweight Docker-inspired container runtime built completely from scratch using **C**, **Linux namespaces**, **chroot**, **kernel modules**, and **system programming primitives**.  
> Designed to demonstrate deep Operating System internals including process isolation, memory monitoring, scheduling, IPC, and concurrent runtime management. :contentReference[oaicite:0]{index=0}

---

## 📌 Overview

**OS Jackfruit** is a custom-built **multi-container runtime** developed as part of an Operating Systems mini project.  
It replicates the foundational concepts behind platforms like **Docker** and **containerd**, but implemented manually at the OS level.

The system consists of two tightly coupled layers:

### 🧩 User-Space Runtime (`engine.c`)
A supervisor process that:

- Spawns isolated containers using `clone()`
- Uses Linux namespaces for isolation
- Provides CLI commands (`start`, `run`, `ps`, `logs`, `stop`)
- Redirects logs asynchronously
- Manages lifecycle of multiple containers
- Communicates with kernel module via `ioctl()`

### ⚙️ Kernel-Space Monitor (`monitor.c`)
A custom Linux kernel module that:

- Tracks container processes
- Monitors RSS memory usage every second
- Enforces soft/hard memory limits
- Sends warnings on breaches
- Kills rogue containers automatically

---

## 🏗️ Architecture

```text
                ┌──────────────────────┐
                │      CLI User        │
                └─────────┬────────────┘
                          │
                          ▼
                ┌──────────────────────┐
                │   Supervisor Engine  │
                │      (engine.c)      │
                └───────┬──────────────┘
                        │
        ┌───────────────┼────────────────┐
        ▼                                ▼
┌──────────────┐                 ┌─────────────────┐
│ Containers   │                 │ Logging System  │
│ clone()+NS   │                 │ Producer/Consumer│
└──────────────┘                 └─────────────────┘
                        │
                        ▼
                ┌──────────────────────┐
                │ Kernel Monitor LKM   │
                │     monitor.c        │
                └──────────────────────┘
```

##🔥 Core Features

🐳 Container Runtime
PID Namespace Isolation
UTS Namespace Isolation
Mount Namespace Isolation
chroot() based filesystem sandboxing
Multiple concurrent containers
Per-container logging
Foreground / background execution

##⚙️ Process Management

Supervisor architecture
Signal handling (SIGCHLD, SIGTERM, SIGINT)
Graceful shutdown
Zombie process cleanup

##🧠 Memory Monitoring

RSS tracking through task_struct
Soft limit warnings
Hard limit auto-kill
Kernel linked-list PID registry

##🧵 Concurrency

Producer-consumer bounded buffer
Mutex + condition variables
Dedicated logger thread

##📊 Scheduling Experiments

Supports CPU / Memory / I/O workload benchmarking.

##🛠️ Tech Stack

Layer	Technologies
Language	C
OS	Linux
IPC	UNIX Domain Sockets
Isolation	clone(), namespaces
Filesystem	chroot()
Monitoring	Linux Kernel Module
Synchronization	pthreads
Logging	Pipes + Threads
Limits	RSS based memory enforcement

##📂 Project Structure

OS-Jackfruit/
│── engine.c              # User-space runtime
│── monitor.c             # Kernel module
│── monitor_ioctl.h       # Shared ioctl definitions
│── logs/                 # Container logs
│── rootfs-base/          # Base container filesystem
│── rootfs-alpha/         # Container image 1
│── rootfs-beta/          # Container image 2
│── workloads/            # cpu_hog / memory_hog / io_pulse
│── README.md

##🚀 Getting Started

1️⃣ Build Runtime
gcc engine.c -o engine -lpthread
2️⃣ Build Kernel Module
make
sudo insmod monitor.ko
3️⃣ Start Supervisor
sudo ./engine supervisor ./rootfs-base
📦 Usage
Start Container
sudo ./engine start alpha ./rootfs-alpha /bin/sh
Run in Foreground
sudo ./engine run beta ./rootfs-beta /bin/sh
List Containers
sudo ./engine ps
View Logs
sudo ./engine logs alpha
Stop Container
sudo ./engine stop alpha
📈 Sample Output
ID        PID    STATE     SOFT(MiB) HARD(MiB)
alpha     3440   running   40        64
beta      3454   running   40        64

##🧪 Workload Experiments

CPU Hog

Stresses scheduler fairness.

Memory Hog

Validates soft/hard memory kill logic.

I/O Pulse

Simulates disk bursts and system responsiveness.

##💡 Engineering Highlights 

✔ Low-Level Systems Design

Implemented mini-Docker architecture from scratch.

✔ Kernel + User Space Integration

Bidirectional communication using ioctl.

✔ Concurrent Logging Pipeline

Bounded-buffer producer-consumer implementation.

✔ Reliable Process Supervision

Graceful reaping, cleanup, lifecycle tracking.

✔ Performance-Oriented Design

Containers launch in milliseconds vs full VMs.

##📚 Operating System Concepts Demonstrated

Process Creation
Scheduling
Signals
Synchronization
IPC
Virtualization
Filesystem Isolation
Kernel Modules
Resource Limits
Memory Management

##⚠️ Limitations

Uses chroot() instead of overlay filesystem
Basic CLI only
No networking namespace yet
No image layering
cgroups can be extended further

##🔮 Future Enhancements

Network namespaces
cgroups v2 support
OverlayFS images
Container snapshots
Web dashboard
Kubernetes-style orchestration

##🏆 Why This Project Stands Out

Most students build apps.
This project builds infrastructure.

It demonstrates skills expected in:
Systems Engineering
Kernel Development
Platform Engineering
Distributed Infrastructure
Runtime / Compiler Teams


##👨‍💻 Authors

Vankadara Sri Kruthi
Valmiki Uma

PES University — Computer Science & Engineering

##📜 License

Academic / Educational Use Only

⭐ If you like this project

Give it a star and fork it.

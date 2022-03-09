#!/usr/bin/python

from __future__ import print_function
from bcc import BPF

# define BPF program
bpf_text = """
#include <uapi/linux/ptrace.h>
struct urandom_read_args {
    // from /sys/kernel/debug/tracing/events/random/urandom_read/format
    u64 __unused__;
    u32 nr_sector;
    u32 bytes;
};
int printarg(struct urandom_read_args *args) {
    bpf_trace_printk("%d\\n", args->bytes);
    return 0;
};
"""

# load BPF program
b = BPF(text=bpf_text)
#b.attach_tracepoint("block:block_rq_issue", "printarg")
b.attach_tracepoint("random:urandom_read", "printarg")

# header
print("%-18s %-16s %-6s %s" % ("TIME(s)", "COMM", "PID", "GOTBITS"))

# format output
while 1:
    try:
        (task, pid, cpu, flags, ts, msg) = b.trace_fields()
    except ValueError:
        continue
    print("%-18.9f %-16s %-6d %s" % (ts, task, pid, msg))

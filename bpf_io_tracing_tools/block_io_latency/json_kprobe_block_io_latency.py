#!/usr/bin/python
# @lint-avoid-python-3-compatibility-imports
#
# biolatency    Summarize block device I/O latency as a histogram.
#       For Linux, uses BCC, eBPF.
#
# USAGE: biolatency [-h] [-T] [-Q] [-m] [-D] [interval] [count]
#
# Copyright (c) 2015 Brendan Gregg.
# Licensed under the Apache License, Version 2.0 (the "License")
#
# 20-Sep-2015   Brendan Gregg   Created this.

from __future__ import print_function
from bcc import BPF
from time import sleep, strftime
import argparse
import ctypes as ct

# arguments
examples = """examples:
    ./biolatency                    # summarize block I/O latency as a histogram
    ./biolatency 1 10               # print 1 second summaries, 10 times
    ./biolatency -mT 1              # 1s summaries, milliseconds, and timestamps
    ./biolatency -Q                 # include OS queued time in I/O time
    ./biolatency -D                 # show each disk device separately
    ./biolatency -F                 # show I/O flags separately
    ./biolatency -j                 # print a dictionary
"""
parser = argparse.ArgumentParser(
    description="Summarize block device I/O latency as a histogram",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=examples)
parser.add_argument("-T", "--timestamp", action="store_true",
                    help="include timestamp on output")
parser.add_argument("-Q", "--queued", action="store_true",
                    help="include OS queued time in I/O time")
parser.add_argument("-m", "--milliseconds", action="store_true",
                    help="millisecond histogram")
parser.add_argument("-D", "--disks", action="store_true",
                    help="print a histogram per disk device")
parser.add_argument("-F", "--flags", action="store_true",
                    help="print a histogram per set of I/O flags")
parser.add_argument("interval", nargs="?", default=99999999,
                    help="output interval, in seconds")
parser.add_argument("count", nargs="?", default=99999999,
                    help="number of outputs")
parser.add_argument("--ebpf", action="store_true",
                    help=argparse.SUPPRESS)
parser.add_argument("-j", "--json", action="store_true",
                    help="json output")

args = parser.parse_args()
countdown = int(args.count)
debug = 0

if args.flags and args.disks:
    print("ERROR: can only use -D or -F. Exiting.")
    exit()

# define BPF program
bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>
typedef struct disk_key {
    char disk[DISK_NAME_LEN];
    u64 slot;
} disk_key_t;
typedef struct flag_key {
    u64 flags;
    u64 slot;
} flag_key_t;
BPF_HASH(start, struct request *);
STORAGE
// time block I/O
int trace_req_start(struct pt_regs *ctx, struct request *req)
{
    u64 ts = bpf_ktime_get_ns();
    start.update(&req, &ts);
    return 0;
}
// output
int trace_req_done(struct pt_regs *ctx, struct request *req)
{
    u64 *tsp, delta;
    // fetch timestamp and calculate delta
    tsp = start.lookup(&req);
    if (tsp == 0) {
        return 0;   // missed issue
    }
    delta = bpf_ktime_get_ns() - *tsp;
    FACTOR
    // store as histogram
    if (delta > 20000) {
        SPILLOVER
    }
    else {
        STORE
    }
    start.delete(&req);
    return 0;
}
"""

# code substitutions
if args.milliseconds:
    bpf_text = bpf_text.replace('FACTOR', 'delta /= 1000000;')
    label = "msecs"
else:
    bpf_text = bpf_text.replace('FACTOR', 'delta /= 1000;')
    label = "usecs"
if args.disks:
    bpf_text = bpf_text.replace('STORAGE',
                                'BPF_HISTOGRAM(dist, disk_key_t);')
    bpf_text = bpf_text.replace('STORE',
                                'disk_key_t key = {.slot = bpf_log2l(delta)}; ' +
                                'void *__tmp = (void *)req->rq_disk->disk_name; ' +
                                'bpf_probe_read_kernel(&key.disk, sizeof(key.disk), __tmp); ' +
                                'dist.increment(key);')
elif args.flags:
    bpf_text = bpf_text.replace('STORAGE',
                                'BPF_HISTOGRAM(dist, flag_key_t);')
    bpf_text = bpf_text.replace('STORE',
                                'flag_key_t key = {.slot = bpf_log2l(delta)}; ' +
                                'key.flags = req->cmd_flags; ' +
                                'dist.increment(key);')
else:
    bpf_text = bpf_text.replace('STORAGE', 'BPF_HISTOGRAM(dist, int, 10001);')
    bpf_text = bpf_text.replace('STORE',
                                'dist.increment(delta%10000);')
    bpf_text = bpf_text.replace('SPILLOVER',
                                'dist.increment(10001);')
if debug or args.ebpf:
    print(bpf_text)
    if args.ebpf:
        exit()

# load BPF program
b = BPF(text=bpf_text)
if args.queued:
    b.attach_kprobe(event="blk_account_io_start", fn_name="trace_req_start")
else:
    if BPF.get_kprobe_functions(b'blk_start_request'):
        b.attach_kprobe(event="blk_start_request", fn_name="trace_req_start")
    b.attach_kprobe(event="blk_mq_start_request", fn_name="trace_req_start")
b.attach_kprobe(event="blk_account_io_done",
                fn_name="trace_req_done")

if not args.json:
    print("Tracing block device I/O... Hit Ctrl-C to end.")

# see blk_fill_rwbs():
req_opf = {
    0: "Read",
    1: "Write",
    2: "Flush",
    3: "Discard",
    5: "SecureErase",
    6: "ZoneReset",
    7: "WriteSame",
    9: "WriteZeros"
}
REQ_OP_BITS = 8
REQ_OP_MASK = ((1 << REQ_OP_BITS) - 1)
REQ_SYNC = 1 << (REQ_OP_BITS + 3)
REQ_META = 1 << (REQ_OP_BITS + 4)
REQ_PRIO = 1 << (REQ_OP_BITS + 5)
REQ_NOMERGE = 1 << (REQ_OP_BITS + 6)
REQ_IDLE = 1 << (REQ_OP_BITS + 7)
REQ_FUA = 1 << (REQ_OP_BITS + 9)
REQ_RAHEAD = 1 << (REQ_OP_BITS + 11)
REQ_BACKGROUND = 1 << (REQ_OP_BITS + 12)
REQ_NOWAIT = 1 << (REQ_OP_BITS + 13)


def flags_print(flags):
    desc = ""
    # operation
    if flags & REQ_OP_MASK in req_opf:
        desc = req_opf[flags & REQ_OP_MASK]
    else:
        desc = "Unknown"
    # flags
    if flags & REQ_SYNC:
        desc = "Sync-" + desc
    if flags & REQ_META:
        desc = "Metadata-" + desc
    if flags & REQ_FUA:
        desc = "ForcedUnitAccess-" + desc
    if flags & REQ_PRIO:
        desc = "Priority-" + desc
    if flags & REQ_NOMERGE:
        desc = "NoMerge-" + desc
    if flags & REQ_IDLE:
        desc = "Idle-" + desc
    if flags & REQ_RAHEAD:
        desc = "ReadAhead-" + desc
    if flags & REQ_BACKGROUND:
        desc = "Background-" + desc
    if flags & REQ_NOWAIT:
        desc = "NoWait-" + desc
    return desc


def _print_json_hist(vals, val_type, section_bucket=None):
    hist_list = []
    max_nonzero_idx = 0
    for i in range(len(vals)):
        if vals[i] != 0:
            max_nonzero_idx = i
    index = 1
    prev = 0
    for i in range(len(vals)):
        if i != 0 and i <= max_nonzero_idx:
            # index = index * 2
            index = index + 2

            list_obj = {}
            list_obj['interval-start'] = prev
            list_obj['interval-end'] = int(index) - 1
            list_obj['count'] = int(vals[i])

            hist_list.append(list_obj)

            prev = index
    histogram = {"ts": strftime("%Y-%m-%d %H:%M:%S"), "val_type": val_type, "data": hist_list}
    if section_bucket:
        histogram[section_bucket[0]] = section_bucket[1]
    print(histogram)


def print_json_hist(self, val_type="value", section_header="Bucket ptr",
                    section_print_fn=None, bucket_fn=None, bucket_sort_fn=None):
    log2_index_max = 10001  # this is a temporary workaround. Variable available in table.py
    if isinstance(self.Key(), ct.Structure):
        tmp = {}
        f1 = self.Key._fields_[0][0]
        f2 = self.Key._fields_[1][0]

        if f2 == '__pad_1' and len(self.Key._fields_) == 3:
            f2 = self.Key._fields_[2][0]
        for k, v in self.items():
            bucket = getattr(k, f1)
            if bucket_fn:
                bucket = bucket_fn(bucket)
            vals = tmp[bucket] = tmp.get(bucket, [0] * log2_index_max)
            slot = getattr(k, f2)
            vals[slot] = v.value
        buckets = list(tmp.keys())
        if bucket_sort_fn:
            buckets = bucket_sort_fn(buckets)
        for bucket in buckets:
            vals = tmp[bucket]
            if section_print_fn:
                section_bucket = (section_header, section_print_fn(bucket))
            else:
                section_bucket = (section_header, bucket)
            _print_json_hist(vals, val_type, section_bucket)

    else:
        vals = [0] * log2_index_max
        for k, v in self.items():
            vals[k.value] = v.value
        _print_json_hist(vals, val_type)


# output
exiting = 0 if args.interval else 1
dist = b.get_table("dist")
while (1):
    try:
        sleep(int(args.interval))
    except KeyboardInterrupt:
        exiting = 1

    print()
    if args.json:
        if args.timestamp:
            print("%-8s\n" % strftime("%H:%M:%S"), end="")

        if args.flags:
            print_json_hist(dist, label, "flags", flags_print)

        else:
            print_json_hist(dist, label)

    else:
        if args.timestamp:
            print("%-8s\n" % strftime("%H:%M:%S"), end="")

        if args.flags:
            dist.print_log2_hist(label, "flags", flags_print)

        else:
            dist.print_log2_hist(label, "disk")

    dist.clear()

    countdown -= 1
    if exiting or countdown == 0:
        exit()
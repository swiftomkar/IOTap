#!/usr/bin/python
# TODO: add -c flag to show r/w ratio by process name
# Kprobe: blk_account_io_start

from bcc import BPF
import argparse
from time import sleep, time

examples = """examples:
sudo python bio_rpm.py        #Get the RPS/RPM for block_io_requests that are being initiated by 
                              #the host machine to the underlying hardware device.
                              #It is possible that this could not end up as an I/O to the device because
                              #of caching and what not 
sudo python bio_rpm.py 1 10   #Print 1 second summaries, 10 times
sudo python bio_rpm -j        # print a dictionary
"""
parser = argparse.ArgumentParser(
    description="Gives the RPS/RPM at the generic block layer only",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=examples)

parser.add_argument("interval", nargs="?", default=99999999,
                    help="output interval, in seconds")
parser.add_argument("count", nargs="?", default=99999999,
                    help="number of outputs")
parser.add_argument("--ebpf", action="store_true",
                    help=argparse.SUPPRESS)
parser.add_argument("--comm", "-c", action="store_true",
                    help=argparse.SUPPRESS)
parser.add_argument("-m", "--minutes", action="store_true",
                    help=argparse.SUPPRESS)
parser.add_argument("-j", "--json", action="store_true",
                    help="json output")

args = parser.parse_args()
countdown = int(args.count)
debug = 0

bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>
#include <linux/sched.h>
typedef struct process_info {
  char comm[TASK_COMM_LEN];  
} proc;
STORAGE

int trace_bio_req_start(struct pt_regs *ctx){
    u64 zero = 0;
    proc process;
    bpf_get_current_comm(&process.comm, sizeof(process.comm));
    INSERT
    INCREMENT
    return 0;
}

"""
# code substitutions
if args.comm:
    bpf_text = bpf_text.replace("STORAGE", "BPF_HASH(count, struct process_info);")
    bpf_text = bpf_text.replace("INSERT", "count.insert(&process, &zero);")
    bpf_text = bpf_text.replace("INCREMENT", "count.increment(process);")
else:
    bpf_text = bpf_text.replace("STORAGE", "BPF_HASH(count);")
    bpf_text = bpf_text.replace("INSERT", "count.insert(&zero, &zero);")
    bpf_text = bpf_text.replace("INCREMENT", "count.increment(zero);")

# tracing
if debug or args.ebpf:
    print(bpf_text)
    if args.ebpf:
        exit()

b = BPF(text=bpf_text)

b.attach_kprobe(event="blk_account_io_start", fn_name="trace_bio_req_start")
start_time = time()
if not args.json:
    print("bio_rpm.py")

exiting = 0 if args.interval else 1

count = b.get_table("count")
while 1:
    try:
        sleep(int(args.interval))
    except KeyboardInterrupt:
        exiting = 1
    total_time = time() - start_time
    if args.comm:
        data = dict()
        for k, v in count.items():
            rps = v.value / total_time
            if args.minutes:

                data[str(k.comm)] = int(rps * 60)
                #print(k.comm, rps * 60)
            else:
                data[str(k.comm)] = int(rps)

                #print(k.comm, rps)
        print(data)
    else:
        rps = count.values()[0].value / total_time
        data = dict()
        if args.minutes:
            if total_time < 60:
                print("Run more than 1 minute for accurate results with -m")
                data['rpm'] = int(rps*60)
            else:
                data['rpm'] = int(rps * 60)
        else:
            data['rps'] = int(rps)
            #print("RPS: ", int(rps))
        print(data)
    countdown -= 1
    if exiting or countdown == 0:
        exit()

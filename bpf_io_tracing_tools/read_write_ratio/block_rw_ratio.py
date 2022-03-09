#!/usr/bin/python
# TODO: add -c flag to show r/w ratio by process name
from bcc import BPF
import argparse
from time import sleep

examples = """examples:
./block_rw_ratio        #Get read:write ratio for time T, where T is the time for which the script is run
./block_rw_ratio 1 10   #Print 1 second summaries, 10 times
"""
parser = argparse.ArgumentParser(
    description="Summarize block device I/O latency as a histogram",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=examples)
parser.add_argument("-c", "--comm", action="store_true",
                    help="print a read:write ratio per process")
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

bpf_text = """
#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>
BPF_HASH(rw);
int trace_req_done (struct pt_regs *ctx, struct request *req)
{
    u64 zero = 0;
    u64 read = 0;
    u64 write = 1;
    u64 req_rw_flag;
    rw.insert(&read, &zero);
    rw.insert(&write, &zero);
#ifdef REQ_WRITE
    req_rw_flag = !!(req->cmd_flags & REQ_WRITE);
#elif defined(REQ_OP_SHIFT)
    req_rw_flag = !!((req->cmd_flags >> REQ_OP_SHIFT) == REQ_OP_WRITE);
#else
    req_rw_flag = !!((req->cmd_flags & REQ_OP_MASK) == REQ_OP_WRITE);
#endif

    if(req_rw_flag == 0){
        rw.increment(read);
    }
    if(req_rw_flag == 1){
        rw.increment(write);
    }
    return 0;
};
"""

if debug or args.ebpf:
    print(bpf_text)
    if args.ebpf:
        exit()

b = BPF(text=bpf_text)

b.attach_kprobe(event="blk_account_io_done", fn_name="trace_req_done")
if not args.json:
    print("block_rw_ratio.py")

exiting = 0 if args.interval else 1
rw = b.get_table("rw")

while 1:
    try:
        sleep(int(args.interval))
    except KeyboardInterrupt:
        exiting = 1
    read = 0
    write = 0
    for k, v in rw.items():
        if k.value == 0:
            read = v.value
        else:
            write = v.value
    data = dict()
    data['read_p'] = round((read/write)*100, 3)
    data['write_p'] = round((1-(read / write))*100, 3)
    data['rw_ratio'] = round(read/write, 3)
    print(data)
    countdown -= 1
    if exiting or countdown == 0:
        exit()
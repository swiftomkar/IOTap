#!/usr/bin/python

from bcc import BPF
import argparse
from time import sleep

examples = """examples:
./avg_blk_io_size        #Get read:write ratio for time T, where T is the time for which the script is run
./avg_blk_io_size 1 10   #Print 1 second summaries, 10 times
"""
parser = argparse.ArgumentParser(
    description="Summarize block device I/O latency as a histogram",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=examples)
parser.add_argument("-c", "--comm", action="store_true",
                    help="print avg block IO size per process")
parser.add_argument("interval", nargs="?", default=99999999,
                    help="output interval, in seconds")
parser.add_argument("count", nargs="?", default=99999999,
                    help="number of outputs")
parser.add_argument("--ebpf", action="store_true",
                    help=argparse.SUPPRESS)
args = parser.parse_args()
countdown = int(args.count)
debug = 0

bpf_text = """

"""

# load BPF program
b = BPF(text=bpf_text)
b.attach_kprobe(event="blk_mq_start_request", fn_name="trace_mq_start_req")



# output
exiting = 0 if args.interval else 1
dist = b.get_table("dist")
while 1:
    try:
        sleep(int(args.interval))
    except KeyboardInterrupt:
        exiting = 1

    dist.print_log2_hist()

    dist.clear()

    countdown -= 1
    if exiting or countdown == 0:
        exit()
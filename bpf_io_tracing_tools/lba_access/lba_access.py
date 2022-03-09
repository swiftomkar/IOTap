from bcc import BPF
import argparse
from time import sleep

examples = """examples:
./lba_access        #Get LBA access for time T, where T is the time for which the script is run
./lba_access 1 10   #Print 1 second summaries, 10 times
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

BPF_HASH(lba);

//BPF_PERF_OUTPUT(overhead);
//u64 time_overhead = 0;

TRACEPOINT_PROBE(block, block_rq_issue) {
    //u64 ts_in;
    //ts_in = bpf_ktime_get_ns();
    u64 zero = 0;
    u64 sec = args->sector;
    lba.insert(&sec, &zero);
    lba.increment(sec);
    //u64 ts_out;
    //ts_out = bpf_ktime_get_ns();
    //time_overhead=time_overhead+(ts_out-ts_in);
    //overhead.perf_submit(args, &time_overhead, sizeof(time_overhead));
    return 0;
};
"""

if debug or args.ebpf:
    print(bpf_text)
    if args.ebpf:
        exit()

b = BPF(text=bpf_text)
#b.attach_tracepoint(tp="block:block_rq_issue", fn_name="trace_req_done")

if not args.json:
    print("block_rw_ratio.py")
exiting = 0 if args.interval else 1
lba = b.get_table("lba")

while 1:
    try:
        sleep(int(args.interval))
    except KeyboardInterrupt:
        exiting = 1

    data = dict()
    #formatting happens here

    for k, v in lba.items():
        data[k.value] = v.value
    if not args.json:
        print("LBA access >1")
        for k,v in data.items():
            if v > 1:
                print(k, v)
    else:
        print(data)
    countdown -= 1
    if exiting or countdown == 0:
        exit()


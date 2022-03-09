import os
from util.utils import UnifiedFormatter

class UnifyMs(UnifiedFormatter):
    def __init__(self):
        super().__init__()
        self.id=0
        self.ms_trace = True
        self.start_ts = 0

    def process_trace(self, line):
        trace = line.split(",")
        for i in range(0, len(trace)):
            trace[i] = trace[i].strip()
        if trace[0] == "DiskRead" or trace[0] == "DiskWrite":
            trace_op = {}
            if self.id == 0:
                self.start_ts = int(trace[1])
            trace_op["id"] = self.id
            self.id += 1
            trace_op["timestamp"] = int(trace[1])-self.start_ts
            trace_op["responseTime"] = int(trace[7])
            if trace[0] == "DiskWrite":
                trace_op["operationType"] = "W"
            else:
                trace_op["operationType"] = "R"
            trace_op["sectorNumber"] = int(trace[5], 0)
            trace_op["ioSize"] = int(trace[6], 0)
            _pid= trace[2].split("(")[1].strip()[:-1]
            trace_op["pid"] = int(_pid)
            return trace_op



if __name__ == "__main__":
    msrc = UnifyMs()
    msrc.add_args()
    msrc.process_args()
    if os.path.isfile(msrc.source_file):
        msrc.get_file_content()

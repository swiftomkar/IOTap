import os
from util.utils import UnifiedFormatter


class UnifyNexus(UnifiedFormatter):
    def __init__(self):
        super().__init__()


    def process_trace(self, line):
        trace = line.split("\t\t")
        #print(trace)
        if not isinstance(float(trace[4]), str):
            trace_op = {}
            if self.id == 0:
                self.start_ts = float(trace[4])
            trace_op["id"] = self.id
            self.id += 1
        for i in range(len(trace)):


            if not isinstance(float(trace[4]), str):
                if i == 4:
                    relative_ts = float(trace[i])-self.start_ts
                    if relative_ts < 0:
                        print("out of order")

                    trace_op["timestamp"] = relative_ts*1000000
                    trace_op["responseTime"] = float(trace[i+3]) - float(trace[i])
                elif i == 3:
                    operation_binary = bin(int(trace[i]))
                    if operation_binary[-1] == '1':
                        trace_op["operationType"] = "W"
                    else:
                        trace_op["operationType"] = "R"
                elif i == 0:
                    trace_op["sectorNumber"] = float(trace[i])*512
                elif i == 2:
                    trace_op["ioSize"] = float(trace[i])
                trace_op["pid"] = "****"
            else:
                print("something is wrong")
        return trace_op


if __name__ == "__main__":
    nexus = UnifyNexus()
    nexus.add_args()
    nexus.process_args()
    if os.path.isfile(nexus.source_file):
        nexus.get_file_content()

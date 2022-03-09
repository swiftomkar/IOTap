from util.utils import UnifiedFormatter
import os


class UnifySsdtrace(UnifiedFormatter):
    def process_trace(self, line):
        # print(line)
        trace = line.split()
        trace_op = {}
        if trace[5]=='Q':
            for i in range(len(trace)):
                try:
                    if i == 2 and trace[i].isnumeric():
                        trace_op["id"] = trace[i]
                    elif i == 3:
                        split = trace[i].split(".")
                        time_sec = float(split[0])
                        time_ns = float(split[1])
                        ts_ms = (time_sec*1000000)+(time_ns/1000)
                        trace_op["timestamp"] = ts_ms#trace[i]
                    elif i == 4 and trace[i].isnumeric():
                        trace_op["pid"] = trace[i]
                    elif i == 6 and trace[i].isalpha():
                        if "R" in trace[i]:
                            trace_op["operationType"] = "R"
                        elif "W" in trace[i]:
                            trace_op["operationType"] = "W"
                        else:
                            pass
                            #trace_op["operationType"] = trace[i]
                    elif i == 7 and trace[i].isnumeric():
                        trace_op["sectorNumber"] = float(trace[i])*512
                        #if trace[6] != "WFS" and trace[6] != "WS":
                        if '+' in line:
                            trace_op["ioSize"] = int(trace[i + 2])*512.0
                        else:
                            trace_op["ioSize"] = 0.0
                    trace_op["responseTime"] = "****"
                except IndexError:
                    print(trace)

            return trace_op
        #print("overriding")


if __name__ == "__main__":
    ssdtrace = UnifySsdtrace()
    ssdtrace.add_args()
    ssdtrace.process_args()
    if os.path.isfile(ssdtrace.source_file):
        ssdtrace.get_file_content()

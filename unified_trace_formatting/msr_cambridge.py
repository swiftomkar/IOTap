import os
from util.utils import UnifiedFormatter

class UnifyMsrc(UnifiedFormatter):
    def __init__(self):
        super().__init__()
        self.id=0

        self.start_ts = 0

    def process_trace(self, line):
        trace = line.split(",")
        try:
            trace[0] = float(trace[0])
        except:
            pass

        if not isinstance(trace[0], str):
            trace_op = {}
            if self.id ==0:
                self.start_ts = trace[0]
            trace_op["id"] = self.id
            self.id += 1
            for i in range(len(trace)):
                try:
                    if i == 0:
                        relative_ts = trace[i]-self.start_ts
                        relative_ts = relative_ts/10
                        if relative_ts < 0:
                            print("out of order")

                        trace_op["timestamp"] = relative_ts
                    # elif i == 4 and trace[i].isnumeric():
                    #    trace_op["pid"] = trace[i]
                    elif i == 6 and trace[i] != "":
                        trace_op["responseTime"] = float(trace[i])/10
                    elif i == 3 and isinstance(trace[i], str):
                        if trace[i] == "Write":
                            trace_op["operationType"] = "W"
                        else:
                            trace_op["operationType"] = "R"
                    elif i == 4 and trace[i].isnumeric():
                        trace_op["sectorNumber"] = int(trace[i])
                        trace_op["ioSize"] = int(trace[i + 1])
                    trace_op["pid"] = "****"
                except IndexError:
                    print(trace)
            #print(trace_op)
            return trace_op
            #print("overriding")


if __name__ == "__main__":
    msrc = UnifyMsrc()
    msrc.add_args()
    msrc.process_args()
    if os.path.isfile(msrc.source_file):
        msrc.get_file_content()
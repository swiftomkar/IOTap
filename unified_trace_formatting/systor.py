import os
from util.utils import UnifiedFormatter


class UnifySystor(UnifiedFormatter):
    def __init__(self):
        super().__init__()
        self.id=0

        self.start_ts = 0

    def process_trace(self, line):
        trace = line.split(",")
        #try:
        #if self.id!=0:
        #    print(trace[0][:-1])
        #    trace[0] = float(trace[0][:-1])
        #except:
        #    print("something is wrong!")
        if trace[0] != 'Timestamp' and (len(trace[0].split('.'))<=2):
            #print(trace)
            trace_op = {}
            #if self.id ==0:
            #    self.start_ts = float(trace[0][:-1])
            #print(self.start_ts)
            trace_op["id"] = self.id
            self.id += 1
            for i in range(len(trace)):
                try:
                    if i == 0:

                        relative_ts = float(trace[i][:-1])-self.start_ts
                        if relative_ts < 0:
                            print("out of order")

                        trace_op["timestamp"] = relative_ts*1000000
                    # elif i == 4 and trace[i].isnumeric():
                    #    trace_op["pid"] = trace[i]
                    elif i == 1 and trace[i] != "":
                        trace_op["responseTime"] = float(trace[i])
                    elif i == 2 and trace[i].isalpha():
                        trace_op["operationType"] = trace[i]
                    elif i == 4 and trace[i].isnumeric():
                        trace_op["sectorNumber"] = int(trace[i])
                        trace_op["ioSize"] = int(trace[i + 1])
                    trace_op["pid"] = "****"
                except IndexError:
                    print(trace)
            #print(trace_op)
            #print(trace_op)
            return trace_op
            #print("overriding")


if __name__ == "__main__":
    systor = UnifySystor()
    systor.add_args()
    systor.process_args()
    if os.path.isfile(systor.source_file):
        systor.get_min_ts(systor.source_file)
        systor.get_file_content()

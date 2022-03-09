import os
import csv
import json
import argparse
from pathlib import Path
#from unified_trace_formatting.ms_prod_ent import UnifyMs


class UnifiedFormatter:
    #__metaclass__ = abc.ABCMeta

    def __init__(self):
        self.source_file = None
        self.destination_file = None
        self.csv_title = None

        self.id=0

        self.start_ts = 0
        self.ms_trace = None

    def add_args(self):
        examples = "ToDo"
        parser = argparse.ArgumentParser(
            description="ToDo",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=examples)
        parser.add_argument("source_path", nargs="?", help="path to source trace")
        parser.add_argument("destination_path", nargs="?", help="destination filepath")
        args = parser.parse_args()

        self.source_file = args.source_path
        self.destination_file = args.destination_path

    def file_iterator(self):
        for file in os.listdir(self.source_file):
            abs_source_file_path = self.source_file + file
            abs_dest_file_path = self.destination_file + file
            #self.ms_trace = True
            print(abs_source_file_path)
            #self.get_min_ts(abs_source_file_path)
            self.get_file_content(abs_source_file_path, abs_dest_file_path)
            self.start_ts = 0
            self.id = 0


    def process_args(self):
        if os.path.isdir(self.source_file):
            print("\ndirectory passed as source, will iterate over all files within\n")
            self.file_iterator()
        else:
            source_file = Path(self.source_file)
            if not source_file.is_file():
                raise FileNotFoundError

            dest_file = Path(self.destination_file)
            if dest_file.is_file():
                raise FileExistsError

            dest_path = os.path.split(self.destination_file)
            path = dest_path[0]
            filename = dest_path[1]

            print("writing output to:", path + '/' + filename)
            if not os.path.exists(path):
                os.makedirs(path)
            #get_file_content(source_filepath, dest_filepath)

    def get_file_content(self, differet_source_file=None, differet_dest_file=None):
        if differet_source_file:
            filecontent_source_file = differet_source_file
        else:
            filecontent_source_file = self.source_file
        if differet_dest_file:
            filecontent_dest_file = differet_dest_file
        else:
            filecontent_dest_file = self.destination_file

        #if not os.path.exists(self.destination_file): #why did I ever write this????
        #    os.makedirs(self.destination_file)
        #print("---->",filecontent_source_file)
        end_header_line = 0
        with open(filecontent_source_file) as source_file:
            if self.ms_trace:
                print("ms trace")
                for line in source_file:
                    if not("EndHeader" in line):
                        end_header_line+=1
                        #continue
                    else:
                        print("end header at line", end_header_line)
                        break
            for line in source_file:
                #print(line)
                trace_obj = self.process_trace(line)
                # print(trace_obj)
                self.common_formatter(trace_obj, filecontent_dest_file)

    #@abc.abstractmethod
    def process_trace(self, line):
        print("you need to override method this accordiung to your specific trace format")
        raise NotImplemented
        # print(line)

    def common_formatter(self, trace_list, output_file_dir):
        if trace_list:
            title = self.get_csv_title()
            with open(output_file_dir, "a+") as opfd:
                cw = csv.DictWriter(opfd, title, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                # cw.writeheader()
                cw.writerow(trace_list)

    def get_csv_title(self):
        if not self.csv_title:
            schema_fp = open("/home/odesai/trace_block_io/schema.json", "r")
            master_schema = json.load(schema_fp)

            self.csv_title = ""
            for key, value in master_schema.items():
                self.csv_title += key + ','
            self.csv_title = self.csv_title[:-1].split(",")
            schema_fp.close()
            return self.csv_title
        else:
            return self.csv_title

    #this function is tested only with systor traces!
    #Although, in general it should work on csv traces where the timestamp is the first attribute
    #PLEASE, PLEASE TEST THIS BEFORE USING ON OTHER TRACES!!!
    def get_min_ts(self, filepath):
        min_ts = float("inf")
        with open(filepath) as source_file:
            for line in source_file:
                if line.split(',')[0]!='Timestamp' and (len(line.split(',')[0].split('.'))<=2):
                    ts=float(line.split(',')[0])
                    if ts<min_ts:
                        min_ts=ts
        self.start_ts = min_ts



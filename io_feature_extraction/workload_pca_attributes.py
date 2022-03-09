#this file contains all fixes to analysis.
#%%

import os
import pandas as pd
import math
import json
import argparse
import glob
#%%
parser = argparse.ArgumentParser(description='Process some traces')
parser.add_argument("source_path", nargs="?", help="path to source trace (only filename)")
args = parser.parse_args()
name = args.source_path#+"/"

#os.chdir("/media/nvme35/unified_traces/systor/"+name)
os.chdir("/media/nvme35/unified_traces/ms_ent/")

#for filename in glob.glob('./*.csv'):
for filename in glob.glob('./*'):
    print("--------------------------------------")
    print(filename)

    #filename = "./"+name
    outfile_name = filename

    pca_attributes = {}
    trace_file_df = pd.read_csv(filename, header=None, usecols=[1,4,5,6])
    trace_file_df.columns = ['ts', 'op', 'ioSize', 'lba']

    print("loaded dataframe")
    #%%
    trace_file_df = trace_file_df.sort_values(by=['ts'], ascending=True)

    #second_01 = int(trace_file_df['ts'][0])-1
    #minute_01 = int(trace_file_df['ts'][0]/60)-1

    #trace_file_df['ts_second'] = trace_file_df.apply(lambda frame:int(frame.ts), axis=1)
    #trace_file_df['ts_minute'] = trace_file_df.apply(lambda frame:int(frame.ts/60), axis=1)
    trace_file_df['ts_second'] = (trace_file_df['ts'] / 1000000).astype(int)
    trace_file_df['ts_minute'] = (trace_file_df['ts'] / 60000000).astype(int)

    #trace_file_df['ioSize'] = trace_file_df['ioSize'].astype('float')
    #trace_file_df['lba'] = trace_file_df['lba'].astype('float')
    print("apply complete")
    #%%

    trace_file_df = trace_file_df.dropna()
    #trace_file_df


    #%%

    #trace_window = trace_file_df['ts'][len(trace_file_df)-1] - trace_file_df['ts'][0]
    #print("time window: ", trace_window,
    #      "number of requests: ", len(trace_file_df)
    #      )

    #%%

    second_groups = trace_file_df.groupby(['ts_second'])
    minute_groups = trace_file_df.groupby(['ts_minute'])

#    if not all(list(minute_groups.count().ts == minute_groups.count().id)):
#        print("Dataset sanity check 1: grouped counts don't match")
    print(len(minute_groups), len(second_groups))

    #%%

    # Read to write ratio
    # this function calculates 3 major attributes:
    # 1 - The ratio of read-write (0: no reads, 1: all reads) for the entire workload
    # 2 - The distribution (max, Q3, median, Q1, min) of the read-write ratio for 1-second windows
    # 3 - The distribution (max, Q3, median, Q1, min) of the read-write ratio for 1-minute windows
    # Total of 1+5+5 attributes for every trace
    # Imp: the inpt this function is a list of tuples.
    # the tuples contain 2 elements.
    # The dataframe is in at index 1 in the tuple.
    # If you want to calculate the RW ratio of a dataframe,
    # pad the input like [(0,<dataframe>)]

    def rw_ratio(groups):
        ratios_across = []
        for group in groups.__iter__():
            a_df = group[1]
            rw_count = a_df['op'].value_counts()
            # print("attn:",group)
            # print("attn:",rw_count)
            #print(rw_count[0])
            ratios_across.append(rw_count[0] / len(a_df.index))
            # ratios_across.append(rw_count[0]/rw_count[1])

        return pd.DataFrame({"rw_ratio": ratios_across})

    #%%

    trace_ratio = rw_ratio([(0, trace_file_df)])
    print("min:", min(trace_ratio.rw_ratio),
          " max:", max(trace_ratio.rw_ratio))
    trace_ratio.rw_ratio.quantile([0.25,0.5,0.75])

    pca_attributes['rwr'] = min(trace_ratio.rw_ratio)

    #%%

    minute_ratios = rw_ratio(minute_groups)
    print("min:", min(minute_ratios.rw_ratio),
          " max:", max(minute_ratios.rw_ratio))
    quartiles = minute_ratios.rw_ratio.quantile([0.25,0.5,0.75])

    pca_attributes['mrwr_max'] = max(minute_ratios.rw_ratio)
    pca_attributes['mrwr_min'] = min(minute_ratios.rw_ratio)
    pca_attributes['mrwr_q1'] = quartiles[0.25]
    pca_attributes['mrwr_q2'] = quartiles[0.50]
    pca_attributes['mrwr_q3'] = quartiles[0.75]
    #%%

    second_ratios = rw_ratio(second_groups)
    print("min:",min(second_ratios.rw_ratio),
          " max:",max(second_ratios.rw_ratio))
    quartiles = second_ratios.rw_ratio.quantile([0.25,0.5,0.75])

    pca_attributes['srwr_max'] = max(second_ratios.rw_ratio)
    pca_attributes['srwr_min'] = min(second_ratios.rw_ratio)
    pca_attributes['srwr_q1'] = quartiles[0.25]
    pca_attributes['srwr_q2'] = quartiles[0.50]
    pca_attributes['srwr_q3'] = quartiles[0.75]


    #a.boxplot(rot=45, fontsize=12, subplots=False)
    print("rw ratio")
    #%%

    def rar_probility(groups):
        rar_count = 0
        total_count = 0
        prev_op_type = "A"
        per_window_probability = []
        for group in groups.__iter__():
            # print(group[1])
            # print(len(group[1].op))
            for op_type in group[1].op:
                # print(len(group[1].op))
                # print(group[1].op[idx])
                if prev_op_type + op_type == "RR":
                    rar_count += 1
                if prev_op_type == "R":
                    total_count += 1
                prev_op_type = op_type
            # print(len(group[1].op)-1)
            if total_count > 0:
                per_window_probability.append(rar_count / total_count)
            else:
                per_window_probability.append(0)
            rar_count = 0
            total_count = 0

        return pd.DataFrame({"rar_probability": per_window_probability})


    #%%

    #minute_ratios = rar_probility(minute_groups)
    full_trace = rar_probility([(0, trace_file_df)])
    print("min:",min(full_trace.rar_probability),
          " max:",max(full_trace.rar_probability))
    full_trace.rar_probability.quantile([0.25,0.5,0.75])

    pca_attributes['rar'] = max(full_trace.rar_probability)

    #%%

    minute_trace = rar_probility(minute_groups)
    print("min:",min(minute_trace.rar_probability),
          " max:",max(minute_trace.rar_probability))
    quartiles = minute_trace.rar_probability.quantile([0.25,0.5,0.75])

    pca_attributes['mrar_max'] = max(minute_trace.rar_probability)
    pca_attributes['mrar_min'] = min(minute_trace.rar_probability)
    pca_attributes['mrar_q1'] = quartiles[0.25]
    pca_attributes['mrar_q2'] = quartiles[0.50]
    pca_attributes['mrar_q3'] = quartiles[0.75]

    #%%

    second_trace = rar_probility(second_groups)
    print("min:", min(second_trace.rar_probability),
          " max:", max(second_trace.rar_probability))

    quartiles = second_trace.rar_probability.quantile([0.25,0.5,0.75])

    pca_attributes['srar_max'] = max(second_trace.rar_probability)
    pca_attributes['srar_min'] = min(second_trace.rar_probability)
    pca_attributes['srar_q1'] = quartiles[0.25]
    pca_attributes['srar_q2'] = quartiles[0.50]
    pca_attributes['srar_q3'] = quartiles[0.75]

    print("rar probability")
    #%%

    def raw_probility(groups):
        rar_count = 0
        total_count = 0
        prev_op_type = "A"
        per_window_probability = []
        for group in groups.__iter__():
            # print(group[1])
            # print(len(group[1].op))
            for op_type in group[1].op:
                # print(len(group[1].op))
                # print(group[1].op[idx])
                if prev_op_type + op_type == "WR":
                    rar_count += 1
                if prev_op_type == "W":
                    total_count += 1
                prev_op_type = op_type
            # print(len(group[1].op)-1)
            if total_count > 0:
                per_window_probability.append(rar_count / total_count)
            else:
                per_window_probability.append(0)
            rar_count = 0
            total_count = 0

        return pd.DataFrame({"raw_probability": per_window_probability})

    #%%

    #minute_ratios = rar_probility(minute_groups)
    full_trace = raw_probility([(0, trace_file_df)])
    print("min:",min(full_trace.raw_probability),
          " max:",max(full_trace.raw_probability))
    full_trace.raw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['raw'] = min(full_trace.raw_probability)

    #%%

    minute_trace = raw_probility(minute_groups)
    print("min:",min(minute_trace.raw_probability),
          " max:",max(minute_trace.raw_probability))
    quartiles = minute_trace.raw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['mraw_max'] = max(minute_trace.raw_probability)
    pca_attributes['mraw_min'] = min(minute_trace.raw_probability)
    pca_attributes['mraw_q1'] = quartiles[0.25]
    pca_attributes['mraw_q2'] = quartiles[0.50]
    pca_attributes['mraw_q3'] = quartiles[0.75]

    #%%

    second_trace = raw_probility(second_groups)
    print("min:",min(second_trace.raw_probability),
          " max:",max(second_trace.raw_probability))

    quartiles = second_trace.raw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['sraw_max'] = max(second_trace.raw_probability)
    pca_attributes['sraw_min'] = min(second_trace.raw_probability)
    pca_attributes['sraw_q1'] = quartiles[0.25]
    pca_attributes['sraw_q2'] = quartiles[0.50]
    pca_attributes['sraw_q3'] = quartiles[0.75]

    print("raw probability")
    #%%

    def war_probility(groups):
        rar_count = 0
        total_count = 0
        prev_op_type = "A"
        per_window_probability = []
        for group in groups.__iter__():
            # print(group)
            # print(len(group[1].op))
            for op_type in group[1].op:
                # print(len(group[1].op))
                # print(group[1].op[idx])
                if prev_op_type + op_type == "RW":
                    rar_count += 1
                if prev_op_type == "R":
                    total_count += 1
                prev_op_type = op_type
            # print(len(group[1].op)-1)
            if total_count > 0:
                per_window_probability.append(rar_count / total_count)
            else:
                per_window_probability.append(0)
            rar_count = 0
            total_count = 0

        return pd.DataFrame({"war_probability": per_window_probability})

    #%%

    #minute_ratios = rar_probility(minute_groups)
    full_trace = war_probility([(0, trace_file_df)])
    print("min:",min(full_trace.war_probability),
          " max:",max(full_trace.war_probability))
    full_trace.war_probability.quantile([0.25,0.5,0.75])

    pca_attributes['war'] = min(full_trace.war_probability)

    #%%

    minute_trace = war_probility(minute_groups)
    print("min:",min(minute_trace.war_probability),
          " max:",max(minute_trace.war_probability))
    quartiles = minute_trace.war_probability.quantile([0.25,0.5,0.75])

    pca_attributes['mwar_max'] = max(minute_trace.war_probability)
    pca_attributes['mwar_min'] = min(minute_trace.war_probability)
    pca_attributes['mwar_q1'] = quartiles[0.25]
    pca_attributes['mwar_q2'] = quartiles[0.50]
    pca_attributes['mwar_q3'] = quartiles[0.75]

    #%%

    second_trace = war_probility(second_groups)
    print("min:", min(second_trace.war_probability),
          " max:", max(second_trace.war_probability))

    quartiles = second_trace.war_probability.quantile([0.25,0.5,0.75])

    pca_attributes['swar_max'] = max(second_trace.war_probability)
    pca_attributes['swar_min'] = min(second_trace.war_probability)
    pca_attributes['swar_q1'] = quartiles[0.25]
    pca_attributes['swar_q2'] = quartiles[0.50]
    pca_attributes['swar_q3'] = quartiles[0.75]

    print("war probability")
    #%%

    def waw_probility(groups):
        rar_count = 0
        total_count = 0
        prev_op_type = "A"
        per_window_probability = []
        for group in groups.__iter__():
            # print(group[1])
            # print(len(group[1].op))
            for op_type in group[1].op:
                # print(len(group[1].op))
                # print(group[1].op[idx])
                if op_type + prev_op_type == "WW":
                    rar_count += 1
                if prev_op_type == "W":
                    total_count += 1
                prev_op_type = op_type
            # print(len(group[1].op)-1)
            if total_count > 0:
                per_window_probability.append(rar_count / total_count)
            else:
                per_window_probability.append(0)
            rar_count = 0
            total_count = 0

        return pd.DataFrame({"waw_probability": per_window_probability})

    #%%

    #minute_ratios = rar_probility(minute_groups)
    full_trace = waw_probility([(0, trace_file_df)])
    print("min:",min(full_trace.waw_probability),
          " max:",max(full_trace.waw_probability))
    full_trace.waw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['waw'] = min(full_trace.waw_probability)

    #%%

    minute_trace = waw_probility(minute_groups)
    print("min:",min(minute_trace.waw_probability),
          " max:",max(minute_trace.waw_probability))
    quartiles = minute_trace.waw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['mwaw_max'] = max(minute_trace.waw_probability)
    pca_attributes['mwaw_min'] = min(minute_trace.waw_probability)
    pca_attributes['mwaw_q1'] = quartiles[0.25]
    pca_attributes['mwaw_q2'] = quartiles[0.50]
    pca_attributes['mwaw_q3'] = quartiles[0.75]

    #%%

    second_trace = waw_probility(second_groups)
    print("min:", min(second_trace.waw_probability),
          " max:", max(second_trace.waw_probability))

    quartiles = second_trace.waw_probability.quantile([0.25,0.5,0.75])

    pca_attributes['swaw_max'] = max(second_trace.waw_probability)
    pca_attributes['swaw_min'] = min(second_trace.waw_probability)
    pca_attributes['swaw_q1'] = quartiles[0.25]
    pca_attributes['swaw_q2'] = quartiles[0.50]
    pca_attributes['swaw_q3'] = quartiles[0.75]

    print("waw probability")
    #%%

    def bps_calc(gropus):
        per_window_bps = []
        for group in gropus.__iter__():
            this_dataframe = group[1]
            group_time = (this_dataframe.ts.iloc[-1] - this_dataframe.ts.iloc[0])/1000000
            if group_time <= 0.0:
                per_window_bps.append(0)
            else:
                #print(this_dataframe.ioSize.sum())
                per_window_bps.append(this_dataframe.ioSize.sum()/group_time)

        return pd.DataFrame({"per_window_bps": per_window_bps})

    #%%


    full_trace_bps = bps_calc([(0, trace_file_df)])
    print("min:",min(full_trace_bps.per_window_bps),
          " max:",max(full_trace_bps.per_window_bps))
    full_trace_bps.per_window_bps.quantile([0.25,0.5,0.75])

    pca_attributes['bps'] = min(full_trace_bps.per_window_bps)
    #%%

    minute_trace_bps = bps_calc(minute_groups)
    print("min:",min(minute_trace_bps.per_window_bps),
          " max:",max(minute_trace_bps.per_window_bps))
    quartiles = minute_trace_bps.per_window_bps.quantile([0.25,0.5,0.75])

    pca_attributes['mbps_max'] = max(minute_trace_bps.per_window_bps)
    pca_attributes['mbps_min'] = min(minute_trace_bps.per_window_bps)
    pca_attributes['mbps_q1'] = quartiles[0.25]
    pca_attributes['mbps_q2'] = quartiles[0.50]
    pca_attributes['mbps_q3'] = quartiles[0.75]

    #%%

    second_trace_bps = bps_calc(second_groups)
    print("min:",min(second_trace_bps.per_window_bps),
          " max:",max(second_trace_bps.per_window_bps))
    quartiles = second_trace_bps.per_window_bps.quantile([0.25,0.5,0.75])

    pca_attributes['sbps_max'] = max(second_trace_bps.per_window_bps)
    pca_attributes['sbps_min'] = min(second_trace_bps.per_window_bps)
    pca_attributes['sbps_q1'] = quartiles[0.25]
    pca_attributes['sbps_q2'] = quartiles[0.50]
    pca_attributes['sbps_q3'] = quartiles[0.75]

    print("bps")
    #%%

    def bwps_calc(groups):
        per_window_bwps = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            group_time = (this_dataframe.ts.iloc[-1] - this_dataframe.ts.iloc[0])/1000000
            if group_time <= 0.0:
                per_window_bwps.append(0)
            else:
                per_window_bwps.append(this_dataframe.loc[this_dataframe['op'] == "W", "ioSize"].sum()/group_time)
        return pd.DataFrame({"bwps": per_window_bwps})

    #%%

    bwps_full_trace = bwps_calc([(0, trace_file_df)])
    print("min:",min(bwps_full_trace.bwps),
          " max:",max(bwps_full_trace.bwps))
    bwps_full_trace.bwps.quantile([0.25,0.5,0.75])

    pca_attributes['bwps'] = min(bwps_full_trace.bwps)

    #%%

    bwps_minute_trace = bwps_calc(minute_groups)
    print("min:",min(bwps_minute_trace.bwps),
          " max:",max(bwps_minute_trace.bwps))
    quartiles = bwps_minute_trace.bwps.quantile([0.25,0.5,0.75])

    pca_attributes['mbwps_max'] = max(bwps_minute_trace.bwps)
    pca_attributes['mbwps_min'] = min(bwps_minute_trace.bwps)
    pca_attributes['mbwps_q1'] = quartiles[0.25]
    pca_attributes['mbwps_q2'] = quartiles[0.50]
    pca_attributes['mbwps_q3'] = quartiles[0.75]

    #%%

    bwps_second_trace = bwps_calc(second_groups)
    print("min:",min(bwps_second_trace.bwps),
          " max:",max(bwps_second_trace.bwps))
    quartiles = bwps_second_trace.bwps.quantile([0.25,0.5,0.75])

    pca_attributes['sbwps_max'] = max(bwps_second_trace.bwps)
    pca_attributes['sbwps_min'] = min(bwps_second_trace.bwps)
    pca_attributes['sbwps_q1'] = quartiles[0.25]
    pca_attributes['sbwps_q2'] = quartiles[0.50]
    pca_attributes['sbwps_q3'] = quartiles[0.75]
    print("bwps")
    #%%

    def brps_calc(groups):
        per_window_brps = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            group_time = (this_dataframe.ts.iloc[-1] - this_dataframe.ts.iloc[0])/1000000
            if group_time <= 0.0:
                per_window_brps.append(0)
            else:
                per_window_brps.append(this_dataframe.loc[this_dataframe['op'] == "R", "ioSize"].sum()/group_time)
        return pd.DataFrame({"brps": per_window_brps})

    #%%

    brps_full_trace = brps_calc([(0, trace_file_df)])
    print("min:",min(brps_full_trace.brps),
          " max:",max(brps_full_trace.brps))
    brps_full_trace.brps.quantile([0.25,0.5,0.75])

    pca_attributes['brps'] = min(brps_full_trace.brps)

    #%%

    brps_minute_trace = brps_calc(minute_groups)
    print("min:",min(brps_minute_trace.brps),
          " max:",max(brps_minute_trace.brps))
    quartiles = brps_minute_trace.brps.quantile([0.25,0.5,0.75])

    pca_attributes['mbrps_max'] = max(brps_minute_trace.brps)
    pca_attributes['mbrps_min'] = min(brps_minute_trace.brps)
    pca_attributes['mbrps_q1'] = quartiles[0.25]
    pca_attributes['mbrps_q2'] = quartiles[0.50]
    pca_attributes['mbrps_q3'] = quartiles[0.75]


    #%%

    brps_second_trace = brps_calc(second_groups)
    print("min:",min(brps_second_trace.brps),
          " max:",max(brps_second_trace.brps))
    quartiles = brps_second_trace.brps.quantile([0.25,0.5,0.75])


    pca_attributes['sbrps_max'] = max(brps_second_trace.brps)
    pca_attributes['sbrps_min'] = min(brps_second_trace.brps)
    pca_attributes['sbrps_q1'] = quartiles[0.25]
    pca_attributes['sbrps_q2'] = quartiles[0.50]
    pca_attributes['sbrps_q3'] = quartiles[0.75]

    print("brps")
    #%%

    def avg_io_size(groups):
        per_window_aio_size = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            per_window_aio_size.append(this_dataframe.ioSize.sum() / this_dataframe.ioSize.count())
        return pd.DataFrame({"aio_size": per_window_aio_size})

    #%%

    aios_full_trace = avg_io_size([(0, trace_file_df)])
    print("min:",min(aios_full_trace.aio_size),
          " max:",max(aios_full_trace.aio_size))
    aios_full_trace.aio_size.quantile([0.25,0.5,0.75])

    pca_attributes['aios'] = min(aios_full_trace.aio_size)
    #%%

    aios_minute_trace = avg_io_size(minute_groups)
    print("min:",min(aios_minute_trace.aio_size),
          " max:",max(aios_minute_trace.aio_size))
    quartiles = aios_minute_trace.aio_size.quantile([0.25,0.5,0.75])

    pca_attributes['maios_max'] = max(aios_minute_trace.aio_size)
    pca_attributes['maios_min'] = min(aios_minute_trace.aio_size)
    pca_attributes['maios_q1'] = quartiles[0.25]
    pca_attributes['maios_q2'] = quartiles[0.50]
    pca_attributes['maios_q3'] = quartiles[0.75]


    #%%

    aios_second_trace = avg_io_size(second_groups)
    print("min:",min(aios_second_trace.aio_size),
          " max:",max(aios_second_trace.aio_size))
    quartiles = aios_second_trace.aio_size.quantile([0.25,0.5,0.75])

    pca_attributes['saios_max'] = max(aios_second_trace.aio_size)
    pca_attributes['saios_min'] = min(aios_second_trace.aio_size)
    pca_attributes['saios_q1'] = quartiles[0.25]
    pca_attributes['saios_q2'] = quartiles[0.50]
    pca_attributes['saios_q3'] = quartiles[0.75]

    print("aios")
    #%%

    def avg_write_size(groups):
        per_window_avg_write_size = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            if len(this_dataframe.loc[this_dataframe['op'] == "W"]) == 0:
                per_window_avg_write_size.append(0)
            else:
                per_window_avg_write_size.append(
                    this_dataframe.loc[this_dataframe['op'] == "W", "ioSize"].sum() /
                    len(this_dataframe.loc[this_dataframe['op'] == "W"]))
        return pd.DataFrame({"aws": per_window_avg_write_size})

    #%%

    aws_full_trace = avg_write_size([(0, trace_file_df)])
    print("min:",min(aws_full_trace.aws),
          " max:",max(aws_full_trace.aws))
    aws_full_trace.aws.quantile([0.25,0.5,0.75])

    pca_attributes['aws'] = min(aws_full_trace.aws)

    #%%

    aws_minute_trace = avg_write_size(minute_groups)
    print("min:",min(aws_minute_trace.aws),
          " max:",max(aws_minute_trace.aws))
    quartiles = aws_minute_trace.aws.quantile([0.25,0.5,0.75])

    pca_attributes['maws_max'] = max(aws_minute_trace.aws)
    pca_attributes['maws_min'] = min(aws_minute_trace.aws)
    pca_attributes['maws_q1'] = quartiles[0.25]
    pca_attributes['maws_q2'] = quartiles[0.50]
    pca_attributes['maws_q3'] = quartiles[0.75]
    #%%

    aws_second_trace = avg_write_size(second_groups)
    print("min:",min(aws_second_trace.aws),
          " max:",max(aws_second_trace.aws))
    quartiles = aws_second_trace.aws.quantile([0.25,0.5,0.75])


    pca_attributes['saws_max'] = max(aws_second_trace.aws)
    pca_attributes['saws_min'] = min(aws_second_trace.aws)
    pca_attributes['saws_q1'] = quartiles[0.25]
    pca_attributes['saws_q2'] = quartiles[0.50]
    pca_attributes['saws_q3'] = quartiles[0.75]

    print("aws")
    #%%

    def avg_read_size(groups):
        per_window_avg_read_size = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            if len(this_dataframe.loc[this_dataframe['op'] == "R"]) == 0:
                per_window_avg_read_size.append(0)
            else:
                per_window_avg_read_size.append(this_dataframe.loc[this_dataframe['op'] == "R",
                                                                   "ioSize"].sum() / len(
                    this_dataframe.loc[this_dataframe['op'] == "R"]))
        return pd.DataFrame({"ars": per_window_avg_read_size})

    #%%

    ars_full_trace = avg_read_size([(0, trace_file_df)])
    print("min:",min(ars_full_trace.ars),
          " max:",max(ars_full_trace.ars))
    ars_full_trace.ars.quantile([0.25,0.5,0.75])

    pca_attributes['ars'] = min(ars_full_trace.ars)

    #%%

    ars_minute_trace = avg_read_size(minute_groups)
    print("min:",min(ars_minute_trace.ars),
          " max:",max(ars_minute_trace.ars))
    quartiles = ars_minute_trace.ars.quantile([0.25,0.5,0.75])

    pca_attributes['mars_max'] = max(ars_minute_trace.ars)
    pca_attributes['mars_min'] = min(ars_minute_trace.ars)
    pca_attributes['mars_q1'] = quartiles[0.25]
    pca_attributes['mars_q2'] = quartiles[0.50]
    pca_attributes['mars_q3'] = quartiles[0.75]
    #%%

    ars_second_trace = avg_read_size(second_groups)
    print("min:",min(ars_second_trace.ars),
          " max:",max(ars_second_trace.ars))
    quartiles = ars_second_trace.ars.quantile([0.25,0.5,0.75])

    pca_attributes['sars_max'] = max(ars_second_trace.ars)
    pca_attributes['sars_min'] = min(ars_second_trace.ars)
    pca_attributes['sars_q1'] = quartiles[0.25]
    pca_attributes['sars_q2'] = quartiles[0.50]
    pca_attributes['sars_q3'] = quartiles[0.75]

    print("ars")
    #%%

    def orms(groups):
        per_window_orms = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            this_dataframe['lba_end'] = (this_dataframe.lba + this_dataframe.ioSize).shift(1)
            this_dataframe['lba_end'] = this_dataframe['lba_end'].fillna(0)

            rms_n = len(this_dataframe.index)
            rms_sum_of_offset_differences_squared = sum(abs(this_dataframe['lba'] - this_dataframe['lba_end']))
            rms = math.sqrt((1 / rms_n) * rms_sum_of_offset_differences_squared)
            per_window_orms.append(rms)
        return pd.DataFrame({"orms": per_window_orms})


    #%%

    orms_full_trace = orms([(0, trace_file_df)])
    print("min:",min(orms_full_trace.orms),
          " max:",max(orms_full_trace.orms))
    orms_full_trace.orms.quantile([0.25,0.5,0.75])

    pca_attributes['orms'] = min(orms_full_trace.orms)

    #%%

    orms_minute_trace = orms(minute_groups)
    print("min:",min(orms_minute_trace.orms),
          " max:",max(orms_minute_trace.orms))
    quartiles = orms_minute_trace.orms.quantile([0.25,0.5,0.75])

    pca_attributes['morms_max'] = max(orms_minute_trace.orms)
    pca_attributes['morms_min'] = min(orms_minute_trace.orms)
    pca_attributes['morms_q1'] = quartiles[0.25]
    pca_attributes['morms_q2'] = quartiles[0.50]
    pca_attributes['morms_q3'] = quartiles[0.75]

    #%%

    orms_second_trace = orms(second_groups)
    print("min:",min(orms_second_trace.orms),
          " max:",max(orms_second_trace.orms))
    quartiles = orms_second_trace.orms.quantile([0.25,0.5,0.75])

    pca_attributes['sorms_max'] = max(orms_second_trace.orms)
    pca_attributes['sorms_min'] = min(orms_second_trace.orms)
    pca_attributes['sorms_q1'] = quartiles[0.25]
    pca_attributes['sorms_q2'] = quartiles[0.50]
    pca_attributes['sorms_q3'] = quartiles[0.75]

    print("orms")
    #%%

    def worms(groups):
        per_window_worms = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            # print(group)
            rms_n = len(this_dataframe.index)
            this_dataframe = this_dataframe.loc[this_dataframe.op == "W"]
            # print(this_dataframe)
            this_dataframe['lba_end'] = (this_dataframe.lba + this_dataframe.ioSize).shift(1)
            this_dataframe['lba_end'] = this_dataframe['lba_end'].fillna(0)

            rms_sum_of_offset_differences_squared = sum(abs(this_dataframe['lba'] - this_dataframe['lba_end']))

            rms = math.sqrt((1 / rms_n) * rms_sum_of_offset_differences_squared)
            per_window_worms.append(rms)
        return pd.DataFrame({"worms": per_window_worms})


    #%%

    worms_full_trace = worms([(0, trace_file_df)])
    print("min:",min(worms_full_trace.worms),
          " max:",max(worms_full_trace.worms))
    worms_full_trace.worms.quantile([0.25,0.5,0.75])

    pca_attributes['worms'] = min(worms_full_trace.worms)

    #%%

    worms_minute_trace = worms(minute_groups)
    print("min:",min(worms_minute_trace.worms),
          " max:",max(worms_minute_trace.worms))
    quartiles = worms_minute_trace.worms.quantile([0.25,0.5,0.75])

    pca_attributes['mworms_max'] = max(worms_minute_trace.worms)
    pca_attributes['mworms_min'] = min(worms_minute_trace.worms)
    pca_attributes['mworms_q1'] = quartiles[0.25]
    pca_attributes['mworms_q2'] = quartiles[0.50]
    pca_attributes['mworms_q3'] = quartiles[0.75]

    #%%

    worms_second_trace = worms(second_groups)
    print("min:",min(worms_second_trace.worms),
          " max:",max(worms_second_trace.worms))
    quartiles = worms_second_trace.worms.quantile([0.25,0.5,0.75])

    pca_attributes['sworms_max'] = max(worms_minute_trace.worms)
    pca_attributes['sworms_min'] = min(worms_minute_trace.worms)
    pca_attributes['sworms_q1'] = quartiles[0.25]
    pca_attributes['sworms_q2'] = quartiles[0.50]
    pca_attributes['sworms_q3'] = quartiles[0.75]

    print("worms")
    #%%

    def rorms(groups):
        per_window_rorms = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            rms_n = len(this_dataframe.index)
            this_dataframe = this_dataframe.loc[this_dataframe.op == "R"]

            this_dataframe['lba_end'] = (this_dataframe.lba + this_dataframe.ioSize).shift(1)
            this_dataframe['lba_end'] = this_dataframe['lba_end'].fillna(0)

            rms_sum_of_offset_differences_squared = sum(abs(this_dataframe['lba'] - this_dataframe['lba_end']))
            rms = math.sqrt((1 / rms_n) * rms_sum_of_offset_differences_squared)
            per_window_rorms.append(rms)
        return pd.DataFrame({"rorms": per_window_rorms})


    #%%

    rorms_full_trace = rorms([(0, trace_file_df)])
    print("min:",min(rorms_full_trace.rorms),
          " max:",max(rorms_full_trace.rorms))
    rorms_full_trace.rorms.quantile([0.25,0.5,0.75])

    pca_attributes['rorms'] = min(rorms_full_trace.rorms)

    #%%

    rorms_minute_trace = rorms(minute_groups)
    print("min:",min(rorms_minute_trace.rorms),
          " max:",max(rorms_minute_trace.rorms))
    quartiles = rorms_minute_trace.rorms.quantile([0.25,0.5,0.75])

    pca_attributes['mrorms_max'] = max(rorms_minute_trace.rorms)
    pca_attributes['mrorms_min'] = min(rorms_minute_trace.rorms)
    pca_attributes['mrorms_q1'] = quartiles[0.25]
    pca_attributes['mrorms_q2'] = quartiles[0.50]
    pca_attributes['mrorms_q3'] = quartiles[0.75]

    #%%

    rorms_second_trace = rorms(second_groups)
    print("min:",min(rorms_second_trace.rorms),
          " max:",max(rorms_second_trace.rorms))
    quartiles = rorms_second_trace.rorms.quantile([0.25,0.5,0.75])

    pca_attributes['srorms_max'] = max(rorms_second_trace.rorms)
    pca_attributes['srorms_min'] = min(rorms_second_trace.rorms)
    pca_attributes['srorms_q1'] = quartiles[0.25]
    pca_attributes['srorms_q2'] = quartiles[0.50]
    pca_attributes['srorms_q3'] = quartiles[0.75]

    print("rorms")

    #%%

    #function to make 3 heatmap jsons/dataframes.
    # 1) full access heatmap
    # 2) Write access heatmap
    # 3) Read access heatmap
    # use these heatmaps to find all xxHOT attributes
    print("heatmap stuff")
    def heatmap(groups):
        heatmap_hot = []
        heatmap_whot = []
        heatmap_rhot = []
        for group in groups.__iter__():
            this_dataframe = group[1]
            group_hot = {}
            group_rhot = {}
            group_whot = {}
            for op, lba in zip(this_dataframe.op, this_dataframe.lba):
                if op == "R":
                    if lba in group_rhot:
                        group_rhot[lba] += 1
                    else:
                        group_rhot[lba] = 1

                if op == "W":
                    if lba in group_whot:
                        group_whot[lba] += 1
                    else:
                        group_whot[lba] = 1

                if lba in group_hot:
                    group_hot[lba] += 1
                else:
                    group_hot[lba] = 1
            #print(group_hot)
            heatmap_hot.append(group_hot)
            heatmap_rhot.append(group_rhot)
            heatmap_whot.append(group_whot)

        return [heatmap_hot, heatmap_rhot, heatmap_whot]

    #%%

    def get_top_io(heatmap_list, percent):
        top10_io_ratios = []
        for heatmap in heatmap_list:
            heatmap_length = len(heatmap)
            top10_len = int((percent/100)*heatmap_length)
            #print(heatmap_length, top10_len)
            i=0
            top10_data_accessed = 0
            total_data_accessed = 0
            for key, val in heatmap.items():
                if i <= top10_len:
                    top10_data_accessed+=val
                total_data_accessed += val
                i+=1
            #print(top10_data_accessed,"/", total_data_accessed)
            if total_data_accessed > 0:
                top10_io_ratios.append(top10_data_accessed/total_data_accessed)
        return pd.DataFrame({"top"+str(percent): top10_io_ratios})


    #%%

    all_full_heatmaps = heatmap([(0, trace_file_df)])
    heatmaps_full_hot = all_full_heatmaps[0]
    heatmaps_full_rhot = all_full_heatmaps[1]
    heatmaps_full_whot = all_full_heatmaps[2]

    #%%

    all_minute_heatmaps = heatmap(minute_groups)
    heatmaps_minute_hot = all_minute_heatmaps[0]
    heatmaps_minute_rhot = all_minute_heatmaps[1]
    heatmaps_minute_whot = all_minute_heatmaps[2]


    #%%

    all_second_heatmaps = heatmap(second_groups)
    heatmaps_second_hot = all_second_heatmaps[0]
    heatmaps_second_rhot = all_second_heatmaps[1]
    heatmaps_second_whot = all_second_heatmaps[2]

    #%%

    hot10_full = get_top_io(heatmaps_full_hot, 10)
    print("min:",min(hot10_full.top10),
          " max:",max(hot10_full.top10))
    hot10_full.top10.quantile([0.25,0.5,0.75])

    pca_attributes['hot10'] = min(hot10_full.top10)

    #%%

    hot25_full = get_top_io(heatmaps_full_hot, 25)
    print("min:",min(hot25_full.top25),
          " max:",max(hot25_full.top25))
    hot25_full.top25.quantile([0.25,0.5,0.75])

    pca_attributes['hot25'] = min(hot25_full.top25)

    #%%

    hot50_full = get_top_io(heatmaps_full_hot, 50)
    print("min:",min(hot50_full.top50),
          " max:",max(hot50_full.top50))
    hot50_full.top50.quantile([0.25,0.5,0.75])

    pca_attributes['hot50'] = min(hot50_full.top50)
    #%%

    rhot10_full = get_top_io(heatmaps_full_rhot, 10)
    print("min:",min(rhot10_full.top10),
          " max:",max(rhot10_full.top10))
    rhot10_full.top10.quantile([0.25,0.5,0.75])

    pca_attributes['rhot10'] = min(rhot10_full.top10)
    #%%

    rhot25_full = get_top_io(heatmaps_full_rhot, 25)
    print("min:",min(rhot25_full.top25),
          " max:",max(rhot25_full.top25))
    rhot25_full.top25.quantile([0.25,0.5,0.75])

    pca_attributes['rhot25'] = min(rhot25_full.top25)
    #%%

    rhot50_full = get_top_io(heatmaps_full_rhot, 50)
    print("min:",min(rhot50_full.top50),
          " max:",max(rhot50_full.top50))
    rhot50_full.top50.quantile([0.25,0.5,0.75])

    pca_attributes['rhot50'] = min(rhot50_full.top50)
    #%%

    whot10_full = get_top_io(heatmaps_full_whot, 10)
    print("min:",min(whot10_full.top10),
          " max:",max(whot10_full.top10))
    whot10_full.top10.quantile([0.25,0.5,0.75])

    pca_attributes['whot10'] = min(whot10_full.top10)
    #%%

    whot25_full = get_top_io(heatmaps_full_whot, 25)
    print("min:",min(whot25_full.top25),
          " max:",max(whot25_full.top25))
    whot25_full.top25.quantile([0.25,0.5,0.75])

    pca_attributes['whot25'] = min(whot25_full.top25)
    #%%

    whot50_full = get_top_io(heatmaps_full_whot, 50)
    print("min:",min(whot50_full.top50),
          " max:",max(whot50_full.top50))
    whot50_full.top50.quantile([0.25,0.5,0.75])

    pca_attributes['whot50'] = min(whot50_full.top50)
    #%%

    ### Minute
    hot10_minute = get_top_io(heatmaps_minute_hot, 10)
    print("min:",min(hot10_minute.top10),
          " max:",max(hot10_minute.top10))
    quartiles = hot10_minute.top10.quantile([0.25,0.5,0.75])

    pca_attributes['mhot10_min'] = min(hot10_minute.top10)
    pca_attributes['mhot10_max'] = max(hot10_minute.top10)
    pca_attributes['mhot10_q1'] = quartiles[0.25]
    pca_attributes['mhot10_q2'] = quartiles[0.50]
    pca_attributes['mhot10_q3'] = quartiles[0.75]


    #%%

    hot25_minute = get_top_io(heatmaps_minute_hot, 25)
    print("min:",min(hot25_minute.top25),
          " max:",max(hot25_minute.top25))
    quartiles = hot25_minute.top25.quantile([0.25,0.5,0.75])

    pca_attributes['mhot25_min'] = min(hot25_minute.top25)
    pca_attributes['mhot25_max'] = max(hot25_minute.top25)
    pca_attributes['mhot25_q1'] = quartiles[0.25]
    pca_attributes['mhot25_q2'] = quartiles[0.50]
    pca_attributes['mhot25_q3'] = quartiles[0.75]


    #%%

    hot50_minute = get_top_io(heatmaps_minute_hot, 50)
    print("min:",min(hot50_minute.top50),
          " max:",max(hot50_minute.top50))
    quartiles = hot50_minute.top50.quantile([0.25,0.5,0.75])

    pca_attributes['mhot50_min'] = min(hot50_minute.top50)
    pca_attributes['mhot50_max'] = max(hot50_minute.top50)
    pca_attributes['mhot50_q1'] = quartiles[0.25]
    pca_attributes['mhot50_q2'] = quartiles[0.50]
    pca_attributes['mhot50_q3'] = quartiles[0.75]

    #%%

    rhot10_minute = get_top_io(heatmaps_minute_rhot, 10)
    print("min:",min(rhot10_minute.top10),
          " max:",max(rhot10_minute.top10))
    quartiles = rhot10_minute.top10.quantile([0.25,0.5,0.75])

    pca_attributes['mrhot10_min'] = min(rhot10_minute.top10)
    pca_attributes['mrhot10_max'] = max(rhot10_minute.top10)
    pca_attributes['mrhot10_q1'] = quartiles[0.25]
    pca_attributes['mrhot10_q2'] = quartiles[0.50]
    pca_attributes['mrhot10_q3'] = quartiles[0.75]

    #%%

    rhot25_minute = get_top_io(heatmaps_minute_rhot, 25)
    print("min:",min(rhot25_minute.top25),
          " max:",max(rhot25_minute.top25))
    quartiles = rhot25_minute.top25.quantile([0.25,0.5,0.75])

    pca_attributes['mrhot25_min'] = min(rhot25_minute.top25)
    pca_attributes['mrhot25_max'] = max(rhot25_minute.top25)
    pca_attributes['mrhot25_q1'] = quartiles[0.25]
    pca_attributes['mrhot25_q2'] = quartiles[0.50]
    pca_attributes['mrhot25_q3'] = quartiles[0.75]
    #%%

    rhot50_minute = get_top_io(heatmaps_minute_rhot, 50)
    print("min:",min(rhot50_minute.top50),
          " max:",max(rhot50_minute.top50))
    quartiles = rhot50_minute.top50.quantile([0.25,0.5,0.75])

    pca_attributes['mrhot50_min'] = min(rhot50_minute.top50)
    pca_attributes['mrhot50_max'] = max(rhot50_minute.top50)
    pca_attributes['mrhot50_q1'] = quartiles[0.25]
    pca_attributes['mrhot50_q2'] = quartiles[0.50]
    pca_attributes['mrhot50_q3'] = quartiles[0.75]

    #%%

    whot10_minute = get_top_io(heatmaps_minute_whot, 10)
    print("min:",min(whot10_minute.top10),
          " max:",max(whot10_minute.top10))
    quartiles = whot10_minute.top10.quantile([0.25,0.5,0.75])

    pca_attributes['mwhot10_min'] = min(whot10_minute.top10)
    pca_attributes['mwhot10_max'] = max(whot10_minute.top10)
    pca_attributes['mwhot10_q1'] = quartiles[0.25]
    pca_attributes['mwhot10_q2'] = quartiles[0.50]
    pca_attributes['mwhot10_q3'] = quartiles[0.75]

    #%%

    whot25_minute = get_top_io(heatmaps_minute_whot, 25)
    print("min:",min(whot25_minute.top25),
          " max:",max(whot25_minute.top25))
    quartiles = whot25_minute.top25.quantile([0.25,0.5,0.75])

    pca_attributes['mwhot25_min'] = min(whot25_minute.top25)
    pca_attributes['mwhot25_max'] = max(whot25_minute.top25)
    pca_attributes['mwhot25_q1'] = quartiles[0.25]
    pca_attributes['mwhot25_q2'] = quartiles[0.50]
    pca_attributes['mwhot25_q3'] = quartiles[0.75]

    #%%

    whot50_minute = get_top_io(heatmaps_minute_whot, 50)
    print("min:",min(whot50_minute.top50),
          " max:",max(whot50_minute.top50))
    quartiles = whot50_minute.top50.quantile([0.25,0.5,0.75])

    pca_attributes['mwhot50_min'] = min(whot50_minute.top50)
    pca_attributes['mwhot50_max'] = max(whot50_minute.top50)
    pca_attributes['mwhot50_q1'] = quartiles[0.25]
    pca_attributes['mwhot50_q2'] = quartiles[0.50]
    pca_attributes['mwhot50_q3'] = quartiles[0.75]


    #%%

    ### Second

    hot10_second = get_top_io(heatmaps_second_hot, 10)
    print("min:",min(hot10_second.top10),
          " max:",max(hot10_second.top10))
    quartiles = hot10_second.top10.quantile([0.25,0.5,0.75])

    pca_attributes['shot10_min'] = min(hot10_second.top10)
    pca_attributes['shot10_max'] = max(hot10_second.top10)
    pca_attributes['shot10_q1'] = quartiles[0.25]
    pca_attributes['shot10_q2'] = quartiles[0.50]
    pca_attributes['shot10_q3'] = quartiles[0.75]
    #%%

    hot25_second = get_top_io(heatmaps_second_hot, 25)
    print("min:",min(hot25_second.top25),
          " max:",max(hot25_second.top25))
    quartiles = hot25_second.top25.quantile([0.25,0.5,0.75])

    pca_attributes['shot25_min'] = min(hot25_second.top25)
    pca_attributes['shot25_max'] = max(hot25_second.top25)
    pca_attributes['shot25_q1'] = quartiles[0.25]
    pca_attributes['shot25_q2'] = quartiles[0.50]
    pca_attributes['shot25_q3'] = quartiles[0.75]

    #%%

    hot50_second = get_top_io(heatmaps_minute_hot, 50)
    print("min:",min(hot50_second.top50),
          " max:",max(hot50_second.top50))
    quartiles = hot50_second.top50.quantile([0.25,0.5,0.75])

    pca_attributes['shot50_min'] = min(hot50_second.top50)
    pca_attributes['shot50_max'] = max(hot50_second.top50)
    pca_attributes['shot50_q1'] = quartiles[0.25]
    pca_attributes['shot50_q2'] = quartiles[0.50]
    pca_attributes['shot50_q3'] = quartiles[0.75]

    #%%

    rhot10_second = get_top_io(heatmaps_second_rhot, 10)
    print("min:",min(rhot10_second.top10),
          " max:",max(rhot10_second.top10))
    quartiles = rhot10_second.top10.quantile([0.25,0.5,0.75])

    pca_attributes['srhot10_min'] = min(rhot10_second.top10)
    pca_attributes['srhot10_max'] = max(rhot10_second.top10)
    pca_attributes['srhot10_q1'] = quartiles[0.25]
    pca_attributes['srhot10_q2'] = quartiles[0.50]
    pca_attributes['srhot10_q3'] = quartiles[0.75]
    #%%

    rhot25_second = get_top_io(heatmaps_second_rhot, 25)
    print("min:",min(rhot25_second.top25),
          " max:",max(rhot25_second.top25))
    quartiles = rhot25_second.top25.quantile([0.25,0.5,0.75])

    pca_attributes['srhot25_min'] = min(rhot10_second.top10)
    pca_attributes['srhot25_max'] = max(rhot10_second.top10)
    pca_attributes['srhot25_q1'] = quartiles[0.25]
    pca_attributes['srhot25_q2'] = quartiles[0.50]
    pca_attributes['srhot25_q3'] = quartiles[0.75]

    #%%

    rhot50_second = get_top_io(heatmaps_minute_rhot, 50)
    print("min:",min(rhot50_second.top50),
          " max:",max(rhot50_second.top50))
    quartiles = rhot50_second.top50.quantile([0.25,0.5,0.75])

    pca_attributes['srhot50_min'] = min(rhot50_second.top50)
    pca_attributes['srhot50_max'] = max(rhot50_second.top50)
    pca_attributes['srhot50_q1'] = quartiles[0.25]
    pca_attributes['srhot50_q2'] = quartiles[0.50]
    pca_attributes['srhot50_q3'] = quartiles[0.75]
    #%%

    whot10_second = get_top_io(heatmaps_second_whot, 10)
    print("min:",min(whot10_second.top10),
          " max:",max(whot10_second.top10))
    quartiles = whot10_second.top10.quantile([0.25,0.5,0.75])

    pca_attributes['swhot10_min'] = min(whot10_second.top10)
    pca_attributes['swhot10_max'] = max(whot10_second.top10)
    pca_attributes['swhot10_q1'] = quartiles[0.25]
    pca_attributes['swhot10_q2'] = quartiles[0.50]
    pca_attributes['swhot10_q3'] = quartiles[0.75]

    #%%

    whot25_second = get_top_io(heatmaps_second_whot, 25)
    print("min:",min(whot25_second.top25),
          " max:",max(whot25_second.top25))
    quartiles = whot25_second.top25.quantile([0.25,0.5,0.75])

    pca_attributes['swhot25_min'] = min(whot25_second.top25)
    pca_attributes['swhot25_max'] = max(whot25_second.top25)
    pca_attributes['swhot25_q1'] = quartiles[0.25]
    pca_attributes['swhot25_q2'] = quartiles[0.50]
    pca_attributes['swhot25_q3'] = quartiles[0.75]

    #%%

    whot50_second = get_top_io(heatmaps_second_whot, 50)
    print("min:",min(whot50_second.top50),
          " max:",max(whot50_second.top50))
    quartiles = whot50_second.top50.quantile([0.25,0.5,0.75])


    pca_attributes['swhot50_min'] = min(whot50_second.top50)
    pca_attributes['swhot50_max'] = max(whot50_second.top50)
    pca_attributes['swhot50_q1'] = quartiles[0.25]
    pca_attributes['swhot50_q2'] = quartiles[0.50]
    pca_attributes['swhot50_q3'] = quartiles[0.75]
    if not os.path.exists("/media/nvme35/analyzed_traces/ms_ent_2/"):
        os.makedirs("/media/nvme35/analyzed_traces/ms_ent_2/")

    with open("/media/nvme35/analyzed_traces/ms_ent_2/"+outfile_name+".json", "w+") as outfile:
        json.dump(pca_attributes, outfile)

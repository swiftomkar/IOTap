#!/bin/bash
#cd ./block_io_latency/ || exit
biolatency="./block_io_latency/json_kprobe_block_io_latency.py"
rpm="./block_requests_per_min_sec/bio_rpm.py"
rwr="./read_write_ratio/block_rw_ratio.py"
lba_access="./lba_access/lba_access.py"
interval=60
#trap '{
#  echo "forked process is $proc_pid. exit"
#  kill $proc_pid
#  exit 1
#  }' INT

while [ true ]; do
  timestamp=$(date +%s)
  mkdir -p "/tmp/trace_block_io_data/${1}/bio_latency/"
  mkdir -p "/tmp/trace_block_io_data/${1}/rpm/"
  mkdir -p "/tmp/trace_block_io_data/${1}/rwr/"
  mkdir -p "/tmp/trace_block_io_data/${1}/lba_access/"
  mkdir -p "/tmp/trace_block_io_data/${1}/smart/"

  chmod -R 666 "/tmp/trace_block_io_data/"

  nohup python3 $biolatency ${interval} 1 -j -Q > "/tmp/trace_block_io_data/${1}/bio_latency/${timestamp}.json" &
  nohup python3 $rpm ${interval} 1 -j > "/tmp/trace_block_io_data/${1}/rpm/${timestamp}.json" &
  nohup python3 $rwr ${interval} 1 -j > "/tmp/trace_block_io_data/${1}/rwr/${timestamp}.json" &
  nohup python3 $lba_access ${interval} 1 -j > "/tmp/trace_block_io_data/${1}/lba_access/${timestamp}.json" &
  smartctl -a -j /dev/sda > "/tmp/trace_block_io_data/${1}/smart/${timestamp}.json"

  sleep $interval
done

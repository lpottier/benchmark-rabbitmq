#!/usr/bin/env bash
# Author: Loic Pottier  <pottier1@llnl.gov>

timestamp () {
  echo "$(date +'%m-%d-%Y %T')@$(hostname):$(basename $BASH_SOURCE)"
}

usage () {
  echo "Usage: $0 NUM_MSGS MSG_SIZE"
  echo -e "\tNUM_MSGS     = Number of messages"
  echo -e "\tMSG_SIZE     = Size of one message (e.g., 50K, 1MB)"
}

if [[ $# -ne 2 ]]; then
  echo "Error: incorrect number of arguments"
  usage
  exit 1
fi

MACHINE=$(echo $(hostname) | sed -e 's/[0-9]*$//')
TS=$(date +%s)

NB_RUNS=5 # Number of runs per data point

# Total size 3.05GB

SIZE="$2"
NUM_ELEM="$1"

# SIZE=50KB
# NUM_ELEM=64000

# SIZE=100KB
# NUM_ELEM=32000

# SIZE=200KB
# NUM_ELEM=16000

# SIZE=400KB
# NUM_ELEM=8000

SIZE=800KB
NUM_ELEM=4000

## NEW
# SIZE=1600KB
# NUM_ELEM=2000

MODE="jeds"

CONF="rmq-${MODE}.json"
OUTPUT_DIR=check-${MODE}-1Xnode-multiqueue
mkdir -p ${OUTPUT_DIR}

start=$(date +"%s")

SCRIPT_DIR="/p/lustre1/pottier1/AMS-Benchmark/scripts/jeds/benchmark-rabbitmq/"

for process in 1 2 4 8 16 32; do
    # In MB
    ID=$(date +%s)
    for avg in $(seq 1 1 ${NB_RUNS}); do
        echo "===== START[${ID}/${avg}] #process=${process} ====="
        RECV_OUTPUT="${OUTPUT_DIR}/jeds-recv-${SIZE}-${NUM_ELEM}-${TS}.csv"
        SEND_OUTPUT="${OUTPUT_DIR}/jeds-send-${SIZE}-${NUM_ELEM}-${TS}.csv"
        # if --strong-scaling then each process send NUM_ELEM/process
        NUM_ELEM_PER_PROC=$((NUM_ELEM/process))
        # So if SS -> recv wait for $NUM_ELEM
        # if not SS -> recv wait for NUM_ELEM * process
        RECV_PROC=$process
        # NUM_ELEM_TOT=$((NUM_ELEM/RECV_PROC)) #if --strong-scaling

        SCALING_OPTS="--strong-scaling"
        # SCALING_OPTS=""

        # exchange
        PROC_SEND=${process}
        # PROC_SEND=100

        TS_UNIQ=$(date +%s)

        ## Exchange topic
        # ${SCRIPT_DIR}bench-recv-topic.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -m ${OUTPUT_DIR}/jeds-recv-${TS}.csv -n ${NUM_ELEM} -sp ${PROC_SEND} -p ${process} &
        # ${SCRIPT_DIR}bench-send.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -s "${SIZE}MB" -m ${OUTPUT_DIR}/jeds-send-${TS}.csv -n ${NUM_ELEM} -p ${PROC_SEND} -topic

        # # Exchange direct
        # ${SCRIPT_DIR}/bench-recv.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${TS_UNIQ} -r ams-test -m ${RECV_OUTPUT} -n ${NUM_ELEM} -t 20 -p ${process} --routing-per-rank &
        # sleep 10s # Needed
        # ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${TS_UNIQ} -r ams-test -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p ${process} --routing-per-rank

        # We assume process can divide NUM_ELEM
        # One queue
        # ${SCRIPT_DIR}/bench-recv.py --config ${CONF} -id ${ID} --queue one-queue-${TS_UNIQ} ${SCALING_OPTS} -m ${RECV_OUTPUT} -n ${NUM_ELEM} -t 30 -p ${process} &
        # ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} -r one-queue-${TS_UNIQ} -s "${SIZE}" ${SCALING_OPTS} -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p ${process}

        # Multi Direct queues with custom routing key
        ${SCRIPT_DIR}/bench-recv-queues.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -q test-rmq-${TS_UNIQ} -m ${RECV_OUTPUT} -n ${NUM_ELEM} -p $process  &
        ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -r test-rmq-${TS_UNIQ} -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p $process  --routing-per-rank

        wait
        echo "===== DONE[${ID}/${avg}] #process=${process} ====="
        sleep 5s
    done
done

end=$(date +"%s")
DIFF=$(($end-$start))
echo "Duration: $(($DIFF / 3600 )) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds"



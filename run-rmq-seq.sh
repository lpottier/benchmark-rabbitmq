#!/usr/bin/env bash
# Author: Loic Pottier  <pottier1@llnl.gov>

timestamp () {
  echo "$(date +'%m-%d-%Y %T')@$(hostname):$(basename $BASH_SOURCE)"
}

usage () {
  echo "Usage: $0 NUM_MSGS MSG_SIZE PROCESS"
  echo -e "\tNUM_MSGS     = Number of messages"
  echo -e "\tMSG_SIZE     = Size of one message (e.g., 50K, 1MB)"
  echo -e "\tPROCESS      = Number of producers processes"
  echo -e "\tPROCESS      = Number of consumers processes"
}

if [[ $# -ne 4 ]]; then
  echo "Error: incorrect number of arguments"
  usage
  exit 1
fi

MACHINE=$(echo $(hostname) | sed -e 's/[0-9]*$//')
TS=$(date +%s)

NB_RUNS=5 # Number of runs per data point

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

# SIZE=800KB
# NUM_ELEM=4000

# NEW
# SIZE=1600KB
# NUM_ELEM=2000

RECV_PROC=$4

METHOD="jeds"
CONF="rmq-${METHOD}.json"
OUTPUT_DIR=benchseq-${METHOD}-2node-${RECV_PROC}
mkdir -p ${OUTPUT_DIR}

start=$(date +"%s")

SCRIPT_DIR="/p/lustre1/pottier1/AMS-Benchmark/scripts/jeds/benchmark-rabbitmq/"

for process in $3; do
    # In MB
    ID=$(date +%s)
    for avg in $(seq 1 1 ${NB_RUNS}); do
        echo "===== START[${ID}/${avg}] #process=${process} ====="
        RECV_OUTPUT="${OUTPUT_DIR}/${METHOD}-recv-${SIZE}-${TS}.csv"
        SEND_OUTPUT="${OUTPUT_DIR}/${METHOD}-send-${SIZE}-${TS}.csv"
        PROC_SEND=${process}
        # NUM_ELEM_TOT=$(( NUM_ELEM * PROC_SEND ))

        SCALING_OPTS="--strong-scaling"
        # SCALING_OPTS=""

        # exchange
        # PROC_SEND=100

        TS_UNIQ=$(date +%s)

        ## Exchange topic
        # ${SCRIPT_DIR}/bench-recv-topic.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r amstopic -m ${RECV_OUTPUT} -n ${NUM_ELEM} -sp ${PROC_SEND} -p ${process} -t 10 -v &
        # ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r amstopic -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p ${PROC_SEND} -topic -v

        QOS=$(( ${NUM_ELEM} > 65535 ? 65535 : ${NUM_ELEM} ))
        QOS=0
        echo "Using NUM_ELEM_TOT= ${NUM_ELEM} | QOS = ${QOS}"

        # Exchange direct
        # ${SCRIPT_DIR}/bench-recv.py --config ${CONF} -id ${ID} -e ams-${TS_UNIQ} -r ams-test -m ${RECV_OUTPUT} -n ${NUM_ELEM_TOT} -t 120 -p ${RECV_PROC} --qos ${QOS} --sender-process ${PROC_SEND} --routing-per-rank  -v &
        # sleep 15s
        # echo "Starting sending data"
        # ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} -e ams-${TS_UNIQ} -r ams-test -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p ${PROC_SEND} --routing-per-rank --sleep 0.1 -v

        # We assume process can divide NUM_ELEM
        # queue

        # # multi queue VALID
        ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} -r one-queue-${TS_UNIQ} -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p ${PROC_SEND} --routing-per-rank -v
        sleep 10s
        ${SCRIPT_DIR}/bench-recv.py --config ${CONF} -id ${ID} --queue one-queue-${TS_UNIQ} -m ${RECV_OUTPUT} -n ${NUM_ELEM} -t 60 -p ${RECV_PROC} --qos ${QOS} --sender-process ${PROC_SEND} --routing-per-rank -v



        # Multi Direct queues with custom routing key
        # ${SCRIPT_DIR}/bench-recv-queues.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -q test-rmq-${TS_UNIQ} -m ${RECV_OUTPUT} -n ${NUM_ELEM} -p $process  &
        # ${SCRIPT_DIR}/bench-send.py --config ${CONF} -id ${ID} ${SCALING_OPTS} -r test-rmq-${TS_UNIQ} -s "${SIZE}" -m ${SEND_OUTPUT} -n ${NUM_ELEM} -p $process  --routing-per-rank

        wait
        echo "===== DONE[${ID}/${avg}] #process=${process} ====="
        #sleep 5s
    done
done

end=$(date +"%s")
DIFF=$(($end-$start))
echo "Duration: $(($DIFF / 3600 )) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds"



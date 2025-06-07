#!/usr/bin/env bash
# Author: Loic Pottier  <pottier1@llnl.gov>

timestamp () {
  echo "$(date +'%m-%d-%Y %T')@$(hostname):$(basename $BASH_SOURCE)"
}

MACHINE=$(echo $(hostname) | sed -e 's/[0-9]*$//')
TS=$(date +%s)

NB_RUNS=3 # Number of runs per data point
SIZE=0.1
NUM_ELEM=32000

OUTPUT_DIR=test-jeds-ex-direct-${TS}
mkdir -p ${OUTPUT_DIR}

start=$(date +"%s")

for process in 1 2 4 8 16 32; do
    # In MB
    ID=$(date +%s)
    for avg in $(seq 1 1 ${NB_RUNS}); do
        echo "===== START[${ID}/${avg}] #process=${process} ====="
        RECV_OUTPUT=${OUTPUT_DIR}/jeds-recv-${TS}-${process}-${avg}.log
        SEND_OUTPUT=${OUTPUT_DIR}/jeds-send-${TS}-${process}-${avg}.log
        # if --strong-scaling then each process send NUM_ELEM/process
        NUM_ELEM_PER_PROC=$((NUM_ELEM/process))
        # So if SS -> recv wait for $NUM_ELEM
        # if not SS -> recv wait for NUM_ELEM * process
        RECV_PROC=$process
        NUM_ELEM_TOT=$((NUM_ELEM/RECV_PROC)) #if --strong-scaling 
        SCALING_OPTS="--strong-scaling"
        # SCALING_OPTS=""

        # exchange
        PROC_SEND=${process}
        PROC_SEND=100

        # ./bench-recv-topic.py --config rmq-jeds.json -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -m ${OUTPUT_DIR}/jeds-recv-${TS}.csv -n ${NUM_ELEM} -sp ${PROC_SEND} -p ${process} &
        # ./bench-send.py --config rmq-jeds.json -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -s "${SIZE}MB" -m ${OUTPUT_DIR}/jeds-send-${TS}.csv -n ${NUM_ELEM} -p ${PROC_SEND} -topic

        ./bench-recv.py --config rmq-jeds.json -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -m ${OUTPUT_DIR}/jeds-recv-${TS}.csv -n ${NUM_ELEM} -p ${process} &
        ./bench-send.py --config rmq-jeds.json -id ${ID} ${SCALING_OPTS} -e ams-${ID} -r ams-test -s "${SIZE}MB" -m ${OUTPUT_DIR}/jeds-send-${TS}.csv -n ${NUM_ELEM} -p ${PROC_SEND}
        
        # We assume process can divide NUM_ELEM
        # queue

        # ./bench-recv.py --config rmq-jeds.json -id ${ID} --queue ams-${ID} ${SCALING_OPTS} -m ${OUTPUT_DIR}/jeds-recv-${TS}.csv -n ${NUM_ELEM} -p ${process} &
        # ./bench-send.py --config rmq-jeds.json -id ${ID} -r ams-${ID} -s "${SIZE}MB" ${SCALING_OPTS} -m ${OUTPUT_DIR}/jeds-send-${TS}.csv -n ${NUM_ELEM} -p ${PROC_SEND}

        wait
        echo "===== DONE[${ID}/${avg}] #process=${process} ====="
    done
done

end=$(date +"%s")
DIFF=$(($end-$start))
echo "Duration: $(($DIFF / 3600 )) hours $((($DIFF % 3600) / 60)) minutes $(($DIFF % 60)) seconds"



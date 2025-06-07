# benchmark-rabbitmq

## Usage to test with using `queues` (no named exchange)

To send `10000` elements of 0.1MB each using 2 processes for the consumers and 2 for the senders:
```bash
./bench-recv.py --config rmq.json -q my-routing-key --strong-scaling -p 2 -n 10000 -m data-recv.csv &
./bench-send.py --config rmq.json -r my-routing-key -s 0.1MB --strong-scaling -p 2 -n 10000 -m data-send.csv
```

> Note here it's `10000` elements total (so 5000 per process). We assume that the number of elements can be divided by the number of processes.

## Usage to test with `direct` exchange

```bash
./bench-recv.py --config rmq.json --strong-scaling -e my-exchange -r my-routing-key -m data-recv.csv -n 10000 -p 2 &
./bench-send.py --config rmq.json --strong-scaling -e my-exchange -r my-routing-key -s 0.1MB -m data-send.csv -n 10000 -p 2
```

## Usage to test with `topic` exchange

Here we use a `topic` exchange where each sending process sends its messages to the binding key `my-routing-key.ID` where ID is the process rank or ID.
The idea is that the consumer can then bind it's queue to a number of these keys, for `my-routing-key.0` and `my-routing-key.1` to receive messages only from rank 0 and 1.

```bash
./bench-recv-topic.py --config rmq.json --strong-scaling -e my-exchange -r my-routing-key -m data-recv.csv -n 10000 -p 2 &
./bench-send.py --config rmq.json --strong-scaling -e my-exchange -r my-routing-key -s 0.1MB -m data-send.csv -n 10000 -p 2 -topic
```

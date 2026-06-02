# Parallel Firewall in C

## Overview

This project implements a multithreaded firewall in C using the producer–consumer model.

Packets are read from an input file, stored in a synchronized ring buffer, and processed concurrently by multiple consumer threads. Each packet is classified as `PASS` or `DROP` based on its source address. The results are written to an output file while preserving the original packet order.

## Main Features

* Parallel packet processing using POSIX threads
* Thread-safe ring buffer implementation
* Producer–consumer synchronization using mutexes and condition variables
* Packet filtering based on allowed source-address ranges
* Packet hash calculation
* Ordered output generation
* Serial implementation included for comparison

## Technologies Used

* C
* POSIX Threads (`pthread`)
* Mutexes and condition variables
* GNU Make

## Build

The `Makefile` builds both the parallel and serial versions:

```bash
make
```

Generated executables:

```text
firewall
serial
```

The project expects the utility files to be available in:

```text
../utils
```

## Run

```bash
./firewall <input-file> <output-file> <num-consumers>
```

The number of consumer threads must be between `1` and `32`.

Example:

```bash
./firewall packets.in results.out 4
```

## Clean

```bash
make clean
```

## Project Structure

```text
.
├── firewall.c       # Parallel firewall entry point
├── serial.c         # Serial implementation
├── producer.c       # Packet producer
├── consumer.c       # Consumer threads and ordered output
├── ring_buffer.c    # Thread-safe ring buffer
├── packet.c         # Packet filtering and hashing
└── Makefile         # Build configuration
```

## Author

**Amalia-Alexandra Călugărița**

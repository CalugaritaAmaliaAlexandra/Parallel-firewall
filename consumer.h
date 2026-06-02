/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include <pthread.h>

#include "ring_buffer.h"
#include "packet.h"

typedef struct so_consumer_ctx_t {
    struct so_ring_buffer_t *producer_rb;

    pthread_mutex_t mutex_dequeue;
    unsigned long next_packet_id;

    pthread_mutex_t write_mutex;
    unsigned long next_log_id;

	pthread_cond_t write_cond;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
                     int num_consumers,
                     so_ring_buffer_t *rb,
                     const char *out_filename);

#endif

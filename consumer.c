// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

static so_consumer_ctx_t context;
static const char *o_filename;

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	int fd = open(o_filename, O_WRONLY | O_CREAT | O_APPEND, 0666);
	int run = 1;

	while (run) {
		char buffer_packet[PKT_SZ];
		ssize_t bytes;
		unsigned long index;

		pthread_mutex_lock(&ctx->mutex_dequeue);

		so_packet_t *packet = (so_packet_t *)buffer_packet;

		bytes = ring_buffer_dequeue(ctx->producer_rb, buffer_packet, PKT_SZ);

		if (bytes == 0) {
			run = 0;
			pthread_mutex_unlock(&ctx->mutex_dequeue);
			continue;
		}

		index = ctx->next_packet_id++;
		pthread_mutex_unlock(&ctx->mutex_dequeue);
		so_action_t action = process_packet(packet);
		unsigned long hash = packet_hash(packet);
		unsigned long timestamp = packet->hdr.timestamp;

		char out_buffer[256];

		snprintf(out_buffer, sizeof(out_buffer), "%s %016lx %lu\n", RES_TO_STR(action), hash, timestamp);

		pthread_mutex_lock(&ctx->write_mutex);

		while (index != ctx->next_log_id)
			pthread_cond_wait(&ctx->write_cond, &ctx->write_mutex);

		write(fd, out_buffer, strlen(out_buffer));

		ctx->next_log_id++;

		pthread_cond_broadcast(&ctx->write_cond);
		pthread_mutex_unlock(&ctx->write_mutex);
	}
	close(fd);
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	o_filename = out_filename;
	context.producer_rb = rb;
	pthread_mutex_init(&context.mutex_dequeue, NULL);
	context.next_packet_id = 0;
	pthread_mutex_init(&context.write_mutex, NULL);
	context.next_log_id = 0;
	pthread_cond_init(&context.write_cond, NULL);

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/

		pthread_create(&tids[i], NULL, (void *)consumer_thread, (void *)&context);
	}

	return num_consumers;
}

// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"

#include <stdlib.h>

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->data = malloc(cap);
	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;
	ring->cap = cap;

	pthread_mutex_init(&ring->mutex, NULL);
	pthread_cond_init(&ring->producer_cond, NULL);
	pthread_cond_init(&ring->consumer_cond, NULL);

	ring->stop = 0;
	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len + size > ring->cap && ring->stop == 0)
		pthread_cond_wait(&ring->producer_cond, &ring->mutex);

	if (ring->stop == 1) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}
	if (ring->write_pos + size <= ring->cap) {
		memcpy(ring->data + ring->write_pos, data, size);
		ring->write_pos = (ring->write_pos + size) % ring->cap;
	} else {
		size_t first_part = ring->cap - ring->write_pos;
		size_t second_part = size - first_part;

		memcpy(ring->data + ring->write_pos, data, first_part);
		memcpy(ring->data, data + first_part, second_part);

		ring->write_pos = second_part;
	}

	ring->len = ring->len + size;

	pthread_cond_signal(&ring->consumer_cond);
	pthread_mutex_unlock(&ring->mutex);

	return (ssize_t)size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len < size && ring->stop == 0)
		pthread_cond_wait(&ring->consumer_cond, &ring->mutex);

	if (ring->len < size && ring->stop == 1) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}

	if (ring->read_pos + size <= ring->cap) {
		memcpy(data, ring->data + ring->read_pos, size);
		ring->read_pos = (ring->read_pos + size) % ring->cap;
	} else {
		size_t first_part = ring->cap - ring->read_pos;
		size_t second_part = size - first_part;

		memcpy(data, ring->data + ring->read_pos, first_part);
		memcpy(data + first_part, ring->data, second_part);

		ring->read_pos = second_part;
	}

	ring->len = ring->len - size;

	pthread_cond_signal(&ring->producer_cond);
	pthread_mutex_unlock(&ring->mutex);

	return (ssize_t)size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	free(ring->data);

	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->producer_cond);
	pthread_cond_destroy(&ring->consumer_cond);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	pthread_mutex_lock(&ring->mutex);

	ring->stop = 1;

	pthread_cond_broadcast(&ring->consumer_cond);
	pthread_cond_broadcast(&ring->producer_cond);

	pthread_mutex_unlock(&ring->mutex);
}

#ifndef UTILS_H_
#define UTILS_H_

#ifdef __cplusplus
extern "C" {
#endif

#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option

#include "czmq.h"
#include <zlog.h>

// Utility macros
#define FREE(x)		do { if ((x) != NULL) {free(x); x=NULL;} } while(0)
#define NULL_UUID	"00000000-0000-0000-0000-000000000000"

// Utility functions
uint32_t 		crc32(uint32_t crc, const void *buf, size_t size);
void			rand_str(char *dst, int size);
char*			gen_uuid_str();
void			dumpzmsg(zlog_category_t* log, zmsg_t *self);
void			sendcmd(pthread_mutex_t *lock, zlog_category_t* log, void *socket, char *from, int command, int arg_length, ...);
char*			uitoa(unsigned int integer);
int				is_zmsg_size_enough(zlog_category_t* log, zmsg_t *msg, int size);

// ---------------------------------------------------------------------
typedef struct _serialize_t serialize_t;

serialize_t*	serialize_create();
void			serialize_destroy(serialize_t *self);
void			serialize_reset(serialize_t *self);
char*			serialize_bufdup(serialize_t *self);
int				serialize_bufset(serialize_t *self, char *buf);

int				serialize_w_delimiter(serialize_t *self);
int				serialize_w_uint64(serialize_t *self, uint64_t src);
int				serialize_w_str(serialize_t *self, char *src);
int				serialize_w_serialize(serialize_t *dst, serialize_t *src);
int				serialize_w_done(serialize_t *self);

//int				serialize_r_prepare(char *src, serialize_t *buf);
int				serialize_r_prepare(serialize_t *buf);
uint64_t		serialize_r_uint64(serialize_t *self);
char*			serialize_r_str(serialize_t *self);

// ---------------------------------------------------------------------
typedef struct _timeout_t timeout_t;
timeout_t*		timeout_create(uint64_t interval_ms);
uint64_t		timeout_remain(timeout_t *self);
void			timeout_update(timeout_t *self);
int				timeout_serialize(timeout_t *self, serialize_t *buf);
timeout_t*		timeout_deserialize(serialize_t *buf);
void			timeout_destroy(timeout_t **self_p);
void			timeout_print(timeout_t *self);

// ---------------------------------------------------------------------
typedef struct _heartbeat_t heartbeat_t;
typedef void	(heartbeat_send_fn)(void *ptr);
heartbeat_t*	heartbeat_create(uint64_t deadtime, uint64_t keepalive, heartbeat_send_fn *send_fn, void *send_param);
int				heartbeat_check(heartbeat_t *self);
void			heartbeat_reset_retries(heartbeat_t *self);
void			heartbeat_reactivate(heartbeat_t *self);
int				heartbeat_serialize(heartbeat_t *self, serialize_t *buf);
heartbeat_t*	heartbeat_deserialize(serialize_t *buf);
void			heartbeat_destroy(heartbeat_t **self_p);

// ---------------------------------------------------------------------
typedef struct _threadpool_t threadpool_t;
typedef void	(threadpool_fn)(void *ptr);
typedef enum {
    threadpool_invalid = -1,
    threadpool_lock_failure = -2,
    threadpool_queue_full = -3,
    threadpool_shutdown = -4,
    threadpool_thread_failure = -5
} threadpool_error_t;
threadpool_t*	threadpool_create(int thread_count, int queue_size, int flags);
int				threadpool_add(threadpool_t *pool, threadpool_fn *function, void *arg, int flags);
int				threadpool_destroy(threadpool_t *pool, int flags);
int				threadpool_task_size(threadpool_t *pool);


#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

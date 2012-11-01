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
void	rand_str(char *dst, int size);
char*	gen_uuid_str();
void	dumpzmsg(zlog_category_t* log, zmsg_t *self);
void	sendcmd(pthread_mutex_t *lock, zlog_category_t* log, void *socket, char *from, int command, int arg_length, ...);
char*	uitoa(unsigned int integer);
int		is_zmsg_size_enough(zlog_category_t* log, zmsg_t *msg, int size);

typedef struct _timeout_t timeout_t;
timeout_t*	timeout_create(uint64_t interval_ms);
uint64_t	timeout_remain(timeout_t *self);
void		timeout_update(timeout_t *self);
void		timeout_destroy(timeout_t **self_p);

typedef struct _heartbeat_t heartbeat_t;
typedef void	(heartbeat_send_fn)(void *ptr);
heartbeat_t*	heartbeat_create(uint64_t deadtime, uint64_t keepalive, heartbeat_send_fn *send_fn, void *send_param);
int				heartbeat_check(heartbeat_t *self);
void			heartbeat_reset_retries(heartbeat_t *self);
void			heartbeat_reactivate(heartbeat_t *self);
void			heartbeat_destroy(heartbeat_t **self_p);

typedef struct threadpool_t threadpool_t;
typedef enum {
    threadpool_invalid = -1,
    threadpool_lock_failure = -2,
    threadpool_queue_full = -3,
    threadpool_shutdown = -4,
    threadpool_thread_failure = -5
} threadpool_error_t;

/**
* @function threadpool_create
* @brief Creates a threadpool_t object.
* @param thread_count Number of worker threads.
* @param queue_size Size of the queue.
* @param flags Unused parameter.
* @return a newly created thread pool or NULL
*/
threadpool_t *threadpool_create(int thread_count, int queue_size, int flags);

typedef void	(threadpool_fn) (void *ptr);
/**
* @function threadpool_add
* @brief add a new task in the queue of a thread pool
* @param pool Thread pool to which add the task.
* @param function Pointer to the function that will perform the task.
* @param argument Argument to be passed to the function.
* @param flags Unused parameter.
* @return 0 if all goes well, negative values in case of error (@see
* threadpool_error_t for codes).
*/
int threadpool_add(threadpool_t *pool, threadpool_fn *function, void *arg, int flags);

/**
* @function threadpool_destroy
* @brief Stops and destroys a thread pool.
* @param pool Thread pool to destroy.
* @param flags Unused parameter.
*/
int threadpool_destroy(threadpool_t *pool, int flags);

int threadpool_task_size(threadpool_t *pool);

#ifdef __cplusplus
}
#endif

#endif /* UTILS_H_ */

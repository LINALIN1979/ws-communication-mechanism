#include "utils.h"
#include "protocol.h"
#include <uuid/uuid.h>

// Fill in random string ([0-9A-Z]) to $dst, assign generated string
// length in $size. Please be careful to use, if $size is N, $dst
// length is at least N+1 (for terminate char '\0').
void
rand_str(char *dst, int size)
{
	static const char text[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	static int firstRun = 1;
	static int text_length = 0;

	if(firstRun) {
		srand(zclock_time());
		text_length = sizeof(text) - 1;
		firstRun = 0;
	}

	if(dst) {
		for(int index = 0; index < size; index++)
			dst[index] = text[rand() % text_length];
		dst[size] = '\0';
	}
}

// Genereate UUID string, remember to free when no more use
char *
gen_uuid_str()
{
	char *uuid_str = (char *)zmalloc(sizeof(char) * 37);
	uuid_t new_uuid;
	uuid_generate(new_uuid);
	uuid_unparse_upper(new_uuid, uuid_str);
	return uuid_str;
}

// Dump zmsg content by frame. Only output the first 10 frames
// for performance concern.
void
dumpzmsg(zlog_category_t *log, zmsg_t *self)
{
	static char *buf = NULL;
	static size_t buf_len = 512;

	if(!buf)
		buf = (char *)zmalloc(sizeof(char) * buf_len);

	zframe_t *frame = zmsg_first(self);
	int frame_nbr = 0;
	int next_write = 0;
	while(frame && frame_nbr++ < 10) {
		byte *data = zframe_data(frame);
		size_t size = zframe_size(frame);

		// Frame is binary format or not?
		int is_bin = 0;
		uint char_nbr;
		for(char_nbr = 0; char_nbr < size; char_nbr++) {
			if(data[char_nbr] < 9 || data[char_nbr] > 127) {
				is_bin = 1;
				break;
			}
		}

		// Prepare memory for output
		size_t frame_total_length = 7 + (is_bin ? size*2 : size+2); // is_bin  - [%03d ...], "["+"%03d"+" "+"]"+"\0" = 7
																	// !is_bin - [%03d "..."], "["+"%03d"+" "+"\""+"\""+"]"+"\0" = 9
		if((buf_len - next_write) < frame_total_length) {
			buf_len = next_write + frame_total_length;
			buf = realloc(buf, buf_len);
		}

		if(buf) {
			// Output starting of frame
			if(!is_bin) { snprintf(buf + next_write, 7, "[%03d \"", (int)size); next_write += 6; }
			else		{ snprintf(buf + next_write, 6, "[%03d ", (int)size);   next_write += 5; }
			// Output content
			for(char_nbr = 0; char_nbr < size; char_nbr++) {
				if(is_bin) {
					snprintf(buf + next_write, 3, "%02X", (unsigned char)data[char_nbr]);
					next_write += 2;
				}
				else {
					snprintf(buf + next_write, 2, "%c", data[char_nbr]);
					next_write++;
				}
			}
			// Output ending of frame
			if(!is_bin) { snprintf(buf + next_write, 3, "\"]");	next_write += 2; }
			else		{ snprintf(buf + next_write, 2, "]");   next_write++; }
		}
		else {
			zlog_error(log, "dumpzmsg failed, output buffer became empty");
			break;
		}

		frame = zmsg_next(self);
	}

	if(!buf) return;

	if(log)	zlog_debug(log, "%s", buf);
	else	printf("%s\n", buf);
}

// Send message through $socket
//
// Parameters:
//   socket - zeromq socket descriptor
//   from - sender type (DISPATCHER, WORKER, CLIENT)
//   command - command type (ex. SERVICEREGREQ, TASKCREATEREP)
//   arg_length - the arguments length of subsequent inputs
//   ... - char arrays to compose multi-part messages to send
void
sendcmd(pthread_mutex_t *lock, zlog_category_t* log, void *socket, char *from, int command, int arg_length, ...)
{
	if(!socket) return;

	zmsg_t *msg = zmsg_new();

	// Data frames:
	if(arg_length > 0) {
		va_list arguments;
		va_start(arguments, arg_length);

		char *tmp;
		for(int index = 0; index < arg_length; index++) {
			tmp = va_arg(arguments, char *);
			if(tmp)	zmsg_addstr(msg, tmp);
			//else	zmsg_addstr(msg, "");
		}
		va_end(arguments);
	}
	// Command frame:
	zmsg_pushstr(msg, cmd_code2payload(command));
	// From frame:
	zmsg_pushstr(msg, from);
	// Empty frame:
	//   DEALER socket has to add empty frame manually before sending
	zmsg_pushstr(msg, "");

	zlog_debug(log, "Sending...");
	dumpzmsg(log, msg);

	// lock mutex if any
	if(lock) {
		if(pthread_mutex_lock(lock) != 0) {
			zlog_error(log, "Unable to lock mutex in sendcmd(), do not send message");
			return;
		}
	}

	if(zmsg_send(&msg, socket))
		zlog_error(log, "Failed to send in zmsg_send(), error code = %d", errno);

	// unlock mutex if any
	if(lock) {
		if(pthread_mutex_unlock(lock) != 0) {
			zlog_error(log,"Failed to unlock mutex in sendcmd(), subsequent messages may be blocked");
			return;
		}
	}
}

// Convert unsigned integer to char array, please remember to free
char *
uitoa(unsigned int value)
{
	size_t buf_len = 16;
	char *buf = (char *)zmalloc(sizeof(char) * buf_len);

	while(buf) {
		int required = snprintf(buf, buf_len, "%u", value);
		if(required >= buf_len) {				// buf is not enough for percentage output, enlarge it
			buf_len = required;
			buf = realloc(buf, buf_len);
		}
		else if(required < 0)	return NULL;	// snprintf error happens
		else					break;			// snprintf success
	}
	return buf;
}

// Check zmsg_t size is larger than or equal to $size or not. Return 1 means
// yes, 0 means not enough and output error.
int
is_zmsg_size_enough(zlog_category_t* log, zmsg_t *msg, int size)
{
	if(!msg) {
		if(log)
			zlog_error(log, "is_zmsg_size_enough: msg ptr is NULL, can't get size");
		else
			printf("is_zmsg_size_enough: msg ptr is NULL, can't get size\n");
		return 0;
	}
	int msg_size = zmsg_size(msg);
	if(msg_size >= size) {
		return 1;
	}
	else {
		if(log)
			zlog_info(log, "is_zmsg_size_enough: msg size is %d which is smaller than %d", msg_size, size);
		else
			printf("is_zmsg_size_enough: msg size is %d which is smaller than %d\n", msg_size, size);
		return 0;
	}
}

struct _timeout_t{
	uint64_t	_old;
	uint64_t	_new;		// Next expiration time

	uint64_t	_interval;
};

// Create timeout_t object.
//
// Parameters:
//   The timeout interval in millisecond
// Return:
//   Created timeout_t object
timeout_t *
timeout_create(uint64_t interval_ms)
{
	timeout_t *ret = (timeout_t *)zmalloc(sizeof(timeout_t));
	if(ret) {
		ret->_interval = interval_ms;
		timeout_update(ret);
	}
	return ret;
}

// Return remaining time in millisecounds.
//
// Return:
//   0 means timeout'd, other return values mean remaining time
uint64_t
timeout_remain(timeout_t *self)
{
	if(self) {
		uint64_t now = zclock_time();
		// Not expired yet, return remaining time
		if(now < self->_new)
			return (self->_new - now);
	}
	return 0;
}

// Update timeout value, it will be expired in $self->_intervalu
// milliseconds from now on
void
timeout_update(timeout_t *self)
{
	if(self) {
		self->_old = zclock_time();
		self->_new = self->_old + self->_interval;
	}
}

void
timeout_destroy(timeout_t **self_p)
{
	if(self_p && *self_p) {
		timeout_t *self = *self_p;
		FREE(self);
		*self_p = NULL;
	}
}

struct _heartbeat_t {
	uint64_t			_deadtime;	// The time (in ms) to judge peer dead or not
	uint64_t			_keepalive;	// The time between heartbeats

	timeout_t			*timeout;
	int					retries;

	heartbeat_send_fn	*send_fn;
	void				*send_param;
};

// Create heartbeat object.
//
// Parameters:
//   deadtime - if doesn't receive peer's heartbeat for $deadtime
//              (millisecond), it will be pronounced dead
//   keepalive - the time between heartbeats
//   send_fn - a callback function for caller to assign what to
//             do for sending heartbeat
//   send_param - parameter for send_fn callback
// Return:
//   Created heartbeat object
heartbeat_t *
heartbeat_create(uint64_t deadtime, uint64_t keepalive, heartbeat_send_fn *send_fn, void *send_param)
{
	heartbeat_t *self = (heartbeat_t *)zmalloc(sizeof(heartbeat_t));
	if(self) {
		self->_deadtime = deadtime;
		self->_keepalive = keepalive;

		self->send_fn = send_fn;
		self->send_param = send_param;
		heartbeat_reset_retries(self);
		self->timeout = timeout_create(self->_keepalive);
	}
	return self;
}

// Do heartbeat check, 1 means still alive, 0 means dead. During
// the check, if it is time to send heartbeat, callback function
// will be called.
//
// Return:
//   1 means alive, 0 means dead.
int
heartbeat_check(heartbeat_t *self)
{
	if(self) {
		// Timeout'd
		if(timeout_remain(self->timeout) == 0) {
			// Retry remains, go retry and update timeout value
			if(self->retries > 0) {
				self->retries--;
				if(self->send_fn)
					self->send_fn(self->send_param);
				timeout_update(self->timeout);
				return 1;
			}
			// No retry remaining, peer was DEAD!!
			else
				return 0;
		}

		// Not timeout'd
		else
			return 1;
	}
	return 0;
}

// Reset retries
void
heartbeat_reset_retries(heartbeat_t *self)
{
	if(self) {
		self->retries = (int)(self->_deadtime / self->_keepalive);
		if((self->_deadtime % self->_keepalive) == 0)
			self->retries--;
	}
}

// This call is to reactivate timeout'd item
void
heartbeat_reactivate(heartbeat_t *self)
{
	if(self) {
		heartbeat_reset_retries(self);
		timeout_update(self->timeout);
	}
}

void
heartbeat_destroy(heartbeat_t **self_p)
{
	if(self_p && *self_p) {
		heartbeat_t *self = *self_p;
		timeout_destroy(&self->timeout);
		free(self);
		*self_p = NULL;
	}
}

// https://github.com/mbrossard/threadpool/tree/master/src
// https://github.com/mbrossard/threadpool/blob/master/src/threadpool.c
// https://github.com/mbrossard/threadpool/blob/master/src/threadpool.h
/**
* @struct threadpool_task
* @brief the work struct
*
* @var function Pointer to the function that will perform the task.
* @var argument Argument to be passed to the function.
*/
typedef struct {
	threadpool_fn *function;
    //void (*function)(void *);
    void *argument;
} threadpool_task_t;

/**
* @struct threadpool
* @brief The threadpool struct
*
* @var notify Condition variable to notify worker threads.
* @var threads Array containing worker threads ID.
* @var thread_count Number of threads
* @var queue Array containing the task queue.
* @var queue_size Size of the task queue.
* @var head Index of the first element.
* @var tail Index of the next element.
* @var shutdown Flag indicating if the pool is shutting down
*/
struct threadpool_t {
  pthread_mutex_t lock;
  pthread_cond_t notify;
  pthread_t *threads;
  threadpool_task_t *queue;
  int thread_count;
  int queue_size;
  int head;
  int tail;
  int count;
  int shutdown;
  int started;
};

/**
* @function void *threadpool_thread(void *threadpool)
* @brief the worker thread
* @param threadpool the pool which own the thread
*/
static void *_threadpool_thread(void *threadpool);

int _threadpool_free(threadpool_t *pool);

threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{
    threadpool_t *pool;
    int i;

    /* TODO: Check for negative or otherwise very big input parameters */

    if((pool = (threadpool_t *)malloc(sizeof(threadpool_t))) == NULL) {
        goto err;
    }

    /* Initialize */
    pool->thread_count = thread_count;
    pool->queue_size = queue_size;
    pool->head = pool->tail = pool->count = 0;
    pool->shutdown = pool->started = 0;

    /* Allocate thread and task queue */
    pool->threads = (pthread_t *)malloc(sizeof (pthread_t) * thread_count);
    pool->queue = (threadpool_task_t *)malloc
        (sizeof (threadpool_task_t) * queue_size);

    /* Initialize mutex and conditional variable first */
    if((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
       (pthread_cond_init(&(pool->notify), NULL) != 0) ||
       (pool->threads == NULL) ||
       (pool->queue == NULL)) {
        goto err;
    }

    /* Start worker threads */
    for(i = 0; i < thread_count; i++) {
        if(pthread_create(&(pool->threads[i]), NULL,
                          _threadpool_thread, (void*)pool) != 0) {
            threadpool_destroy(pool, 0);
            return NULL;
        } else {
            pool->started++;
        }
    }

    return pool;

 err:
    if(pool) {
        _threadpool_free(pool);
    }
    return NULL;
}

int threadpool_task_size(threadpool_t *pool)
{
	if(pool)
		return pool->count;
	return 0;
}

int threadpool_add(threadpool_t *pool, threadpool_fn *function,
                   void *argument, int flags)
{
    int err = 0;
    int next;

    if(pool == NULL || function == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    next = pool->tail + 1;
    next = (next == pool->queue_size) ? 0 : next;

    do {
        /* Are we full ? */
        if(pool->count == pool->queue_size) {
            err = threadpool_queue_full;
            break;
        }

        /* Are we shutting down ? */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        /* Add task to queue */
        pool->queue[pool->tail].function = function;
        pool->queue[pool->tail].argument = argument;
        pool->tail = next;
        pool->count += 1;

        /* pthread_cond_broadcast */
        if(pthread_cond_signal(&(pool->notify)) != 0) {
            err = threadpool_lock_failure;
            break;
        }
    } while(0);

    if(pthread_mutex_unlock(&pool->lock) != 0) {
        err = threadpool_lock_failure;
    }

    return err;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{
    int i, err = 0;

    if(pool == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    do {
        /* Already shutting down */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        pool->shutdown = 1;

        /* Wake up all worker threads */
        if((pthread_cond_broadcast(&(pool->notify)) != 0) ||
           (pthread_mutex_unlock(&(pool->lock)) != 0)) {
            err = threadpool_lock_failure;
            break;
        }

        /* Join all worker thread */
        for(i = 0; i < pool->thread_count; i++) {
            if(pthread_join(pool->threads[i], NULL) != 0) {
                err = threadpool_thread_failure;
            }
        }
    } while(0);

    if(pthread_mutex_unlock(&pool->lock) != 0) {
        err = threadpool_lock_failure;
    }

    /* Only if everything went well do we deallocate the pool */
    if(!err) {
        _threadpool_free(pool);
    }
    return err;
}

int _threadpool_free(threadpool_t *pool)
{
    if(pool == NULL || pool->started > 0) {
        return -1;
    }

    /* Did we manage to allocate ? */
    if(pool->threads) {
        free(pool->threads);
        free(pool->queue);

        /* Because we allocate pool->threads after initializing the
mutex and condition variable, we're sure they're
initialized. Let's lock the mutex just in case. */
        pthread_mutex_lock(&(pool->lock));
        pthread_mutex_destroy(&(pool->lock));
        pthread_cond_destroy(&(pool->notify));
    }
    free(pool);
    return 0;
}

static void *_threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;
    threadpool_task_t task;

    for(;;) {
        /* Lock must be taken to wait on conditional variable */
        pthread_mutex_lock(&(pool->lock));

        /* Wait on condition variable, check for spurious wakeups.
When returning from pthread_cond_wait(), we own the lock. */
        while((pool->count == 0) && (!pool->shutdown)) {
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        if(pool->shutdown) {
            break;
        }

        /* Grab our task */
        task.function = pool->queue[pool->head].function;
        task.argument = pool->queue[pool->head].argument;
        pool->head += 1;
        pool->head = (pool->head == pool->queue_size) ? 0 : pool->head;
        pool->count -= 1;

        /* Unlock */
        pthread_mutex_unlock(&(pool->lock));

        /* Get to work */
        (*(task.function))(task.argument);
    }

    pool->started--;

    pthread_mutex_unlock(&(pool->lock));
    pthread_exit(NULL);
    return(NULL);
}


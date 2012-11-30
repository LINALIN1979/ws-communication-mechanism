#ifndef TASK_H_
#define TASK_H_

#include "zmq.h"
#include "czmq.h"
#include "../wscommon/utils.h"

// If task idle longer than this interval (in millisecond),
// task status will be set to FAIL
#define TASK_IDLE_TIME_BEFORE_BECOME_FAIL	3600000

typedef struct _task_t task_t;

task_t*	task_create(char *service_name, zframe_t *client, char *method, char *data, zframe_t *worker);
task_t*	task_create_manually(char *taskID, unsigned int status, int dispatched,
			uint64_t createTime, uint64_t timeout_old, uint64_t timeout_new, uint64_t timeout_interval,
			char *serviceName, char *method, char *data,
			char *client_str, char *worker_str, int assigned_worker);
void	task_destroy(void *argument);

int		task_get_status(task_t *self);
void	task_set_status(task_t *self, int status);

int		task_get_dispatched(task_t *self);
void	task_set_dispatched(task_t *self, int dispatched);

zframe_t*	task_get_client(task_t *self);
char*		task_get_client_str(task_t *self);
void		task_set_client(task_t *self, zframe_t *client);
void		task_set_client_str(task_t *self, char *client_str);

zframe_t*	task_get_worker(task_t *self);
char*		task_get_worker_str(task_t *self);
void		task_set_worker(task_t *self, zframe_t *worker);
void		task_set_worker_str(task_t *self, char *worker_str);

char*	task_get_taskID(task_t *self);
uint64_t	task_get_createTime(task_t *self);
timeout_t*	task_get_timeout(task_t *self);
char*	task_get_serviceName(task_t *self);
char*	task_get_method(task_t *self);
char*	task_get_data(task_t *self);
int		task_get_expired(task_t *self);
int		task_serialize(task_t *self, serialize_t *buf);
task_t*	task_deserialize(serialize_t *buf);
void	task_print(task_t * self);
int		task_get_assigned_worker(task_t *self);

#endif /* TASK_H_ */

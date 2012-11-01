#ifndef TASK_H_
#define TASK_H_

#include "zmq.h"
#include "czmq.h"

// If task idle longer than this interval (in millisecond),
// task status will be set to FAIL
#define TASK_IDLE_TIME_BEFORE_BECOME_FAIL	20000

typedef struct _task_t task_t;

task_t*	task_create(char *service_name, zframe_t *client, char *method, char *data, zframe_t *worker);
void	task_destroy(void *argument);

int		task_get_status(task_t *self);
void	task_set_status(task_t *self, int status);

int		task_get_dispatched(task_t *self);
void	task_set_dispatched(task_t *self, int dispatched);

zframe_t*	task_get_client(task_t *self);
char*		task_get_clientstr(task_t *self);
void		task_set_client(task_t *self, zframe_t *client);

zframe_t*	task_get_worker(task_t *self);
char*		task_get_workerstr(task_t *self);
void		task_set_worker(task_t *self, zframe_t *worker);

char*	task_get_taskID(task_t *self);
char*	task_get_servicename(task_t *self);
char*	task_get_method(task_t *self);
char*	task_get_data(task_t *self);
int		task_get_expired(task_t *self);

#endif /* TASK_H_ */

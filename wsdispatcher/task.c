#include "task.h"
#include "../wscommon/utils.h"
#include "../wscommon/protocol.h"

// structure of task
struct _task_t {
	char			*taskID;		// task ID
	unsigned int	status;			// task status (0~100, FAIL, DISPATCHING)

	int				dispatched;		// task was passed to worker

	int64_t			createTime;		// task create time
	timeout_t		*timeout;

	char			*serviceName;	// service name which serves the task
	char			*method;
	char			*data;			// any other data in addition to status

	zframe_t		*client;		// client address who create the task
	char			*client_str;
	zframe_t		*worker;		// worker address who handles the task
	char			*worker_str;
};

int
task_serialize(task_t *self, serialize_t *buf)
{
	if(self && buf) {
		// char *taskID
		if(serialize_w_str(buf, self->taskID) != 0) goto task_serialize_err;
		// unsigned int	status
		if(serialize_w_uint64(buf, self->status) != 0) goto task_serialize_err;
		// int dispatched
		if(serialize_w_uint64(buf, self->dispatched) != 0) goto task_serialize_err;
		// int64_t createTime
		if(serialize_w_uint64(buf, self->createTime) != 0) goto task_serialize_err;
		// timeout_t *timeout
		serialize_t *buf2 = serialize_create();
		if(!buf2) goto task_serialize_err;
		else {
			if(timeout_serialize(self->timeout, buf2) != 0) {
				serialize_destroy(buf2);
				goto task_serialize_err;
			}
			if(serialize_w_serialize(buf, buf2) != 0) {
				serialize_destroy(buf2);
				goto task_serialize_err;
			}
			serialize_destroy(buf2);
		}
		// char *serviceName
		if(serialize_w_str(buf, self->serviceName) != 0) goto task_serialize_err;
		// char *method
		if(serialize_w_str(buf, self->method) != 0) goto task_serialize_err;
		// char *data
		if(serialize_w_str(buf, self->data) != 0) goto task_serialize_err;
		// zframe_t *client
		// ...
		// char *client_str
		if(serialize_w_str(buf, self->client_str) != 0) goto task_serialize_err;
		// zframe_t *worker
		// ...
		// char	*worker_str
		if(serialize_w_str(buf, self->worker_str) != 0) goto task_serialize_err;

		// TODO: add CRC support

		serialize_w_done(buf);
	}
	return 0;

task_serialize_err:
	return 1;
}

task_t *
task_deserialize(serialize_t *buf)
{
	task_t *self = NULL;
	if(buf) {
		if(serialize_r_prepare(buf) == 0) {
			self = (task_t *)zmalloc(sizeof(task_t));
			if(self) {
				// char *taskID
				if((self->taskID = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("taskID %s\n", self->taskID);
				// unsigned int	status
				self->status = (unsigned int)serialize_r_uint64(buf);
				//printf("status %u\n", self->status);
				// int dispatched
				self->dispatched = serialize_r_uint64(buf);
				//printf("dispatched %d\n", self->dispatched);
				// int64_t createTime
				self->createTime = serialize_r_uint64(buf);
				//printf("createTime %016lu\n", self->createTime);
				// timeout_t *timeout
				self->timeout = timeout_deserialize(buf);
				if(self->timeout == NULL)	goto task_deserialize_err;
				//else printf("got timeout\n");
				// char *serviceName
				if((self->serviceName = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("serviceName %s\n", self->serviceName);
				// char *method
				if((self->method = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("method %s\n", self->method);
				// char *data
				if((self->data = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("data %s\n", self->data);
				// zframe_t *client
				// ...
				// char *client_str
				if((self->client_str = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("client_str %s\n", self->client_str);
				self->client = zframe_new(self->client_str, strlen(self->client_str));
				// zframe_t *worker
				// ...
				// char	*worker_str
				if((self->worker_str = serialize_r_str(buf)) == NULL) goto task_deserialize_err;
				//else printf("worker_str %s\n", self->worker_str);
				self->worker = zframe_new(self->worker_str, strlen(self->worker_str));
			}
		}
	}
	return self;

task_deserialize_err:
	if(self)	task_destroy(self);
	return NULL;
}

task_t *
task_create(char *service_name, zframe_t *client, char *method, char *data, zframe_t *worker)
{
	task_t *task = (task_t *)zmalloc(sizeof(task_t));
	if(task) {
		task->taskID = gen_uuid_str();
		task->status = DISPATCHING;

		task->dispatched = 0;

		task->createTime = zclock_time();
		task->timeout = timeout_create(TASK_IDLE_TIME_BEFORE_BECOME_FAIL);

		task->serviceName = strdup(service_name);
		task->method = strdup(method);
		task->data = strdup(data);

		task_set_client(task, client);
		task_set_worker(task, worker); // If has specific worker, assigned here. If no, service_dispatch() will handle it

		timeout_update(task->timeout);
	}
	return task;
}

void
task_destroy(void *argument)
{
    task_t *self = (task_t *)argument;
    if(self) {
		FREE(self->taskID);

		timeout_destroy(&self->timeout);

		FREE(self->serviceName);
		FREE(self->method);
		FREE(self->data);

		if(self->client) zframe_destroy(&self->client);
		FREE(self->client_str);
		if(self->worker) zframe_destroy(&self->worker);
		FREE(self->worker_str);

		free(self);
    }
}

int
task_get_status(task_t *self)
{
	if(self)return self->status;
	else	return FAIL;
}

void
task_set_status(task_t *self, int status)
{
	if(self) {
		// Can touch the task which already done (i.e. status == 100 || FAIL)
		if(self->status == 100 || self->status == FAIL) return;

		// TODO: should we have to do nothing when new status value is the same as previous?
		//if(status == self->status) return;

		if((status > 100) && (status != FAIL) && (status != DISPATCHING))
			self->status = 100;
		else
			self->status = status;
		timeout_update(self->timeout);
	}
}

int
task_get_dispatched(task_t *self)
{
	if(self)return self->dispatched;
	else	return 0;
}

void
task_set_dispatched(task_t *self, int dispatched)
{
	if(self) {
		// TODO: should we have to do nothing when new dispatched value is the same as previous?

		if(dispatched <= 0)	self->dispatched = 0;
		else				self->dispatched = 1;
		timeout_update(self->timeout);
	}
}

zframe_t*
task_get_client(task_t *self)
{
	if(self)return self->client;
	else	return NULL;
}

char*
task_get_clientstr(task_t *self)
{
	if(self)return self->client_str;
	else	return NULL;
}

void
task_set_client(task_t *self, zframe_t *client)
{
	if(self && client) {
		self->client = zframe_dup(client);
		self->client_str = zframe_strdup(self->client);
		timeout_update(self->timeout);
	}
}

timeout_t*
task_get_timeout(task_t *self)
{
	if(self)return self->timeout;
	else	return NULL;
}

zframe_t*
task_get_worker(task_t *self)
{
	if(self)return self->worker;
	else	return NULL;
}

char*
task_get_workerstr(task_t *self)
{
	if(self)return self->worker_str;
	else	return NULL;
}

void
task_set_worker(task_t *self, zframe_t *worker)
{
	if(self && worker) {
		self->worker = zframe_dup(worker);
		self->worker_str = zframe_strdup(self->worker);
		timeout_update(self->timeout);
	}
}

char*
task_get_taskID(task_t *self)
{
	if(self)return self->taskID;
	else	return NULL;
}

char*
task_get_servicename(task_t *self)
{
	if(self)return self->serviceName;
	else	return NULL;
}

char*
task_get_method(task_t *self)
{
	if(self)return self->method;
	else	return NULL;
}

char*
task_get_data(task_t *self)
{
	if(self)return self->data;
	else	return NULL;
}

int
task_get_expired(task_t *self)
{
	if(self) {
		if(timeout_remain(self->timeout) > 0)
			return 0;
	}
	return 1;
}

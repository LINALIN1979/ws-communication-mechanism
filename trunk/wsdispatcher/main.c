// TODO: secure channel
// TODO: integrate authentication, authorization
// TODO: serialization

#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option

#include "../wscommon/protocol.h"
#include "../wscommon/utils.h"
#include "task.h"

#define TASKPROC_IN_MULTITHREAD 1

#define HEARTBEAT_LIVEING	15000	// If doesn't receive peer heartbeat exceeds this period, will treat as dead
#define HEARTBEAT_INTERVAL  3000	// Heartbeat sending interval (in millisecond)

// ----------------------------------------------------------
typedef struct {
	zctx_t			*ctx;			// Context
	char			*bind;			// Dispatcher bind address
	void			*socket;		// Socket for clients & workers to connect

	zhash_t			*workers;		// Hash table to store connected workers
	zhash_t			*services;		// Hash of known services
	zhash_t			*tasks;			// Hash of tasks

	zlog_category_t	*log;

#if TASKPROC_IN_MULTITHREAD
	threadpool_t	*threads;
	pthread_mutex_t	sock_lock;
#endif
} dispatcher_t;

static dispatcher_t *
	dispatcher_new();
static void
	dispatcher_destroy(dispatcher_t **self_p);

// ----------------------------------------------------------
typedef struct {
	dispatcher_t	*dispatcher;	// Dispatcher instance
    char			*name;			// Service name
    zlist_t			*workers;		// Queue of on-duty workers

    zlist_t			*tasks;			// List of ongoing tasks (status != 100 or FAIL)
} service_t;

static service_t *
	service_require (dispatcher_t *self, zframe_t *service_frame);
static void
	service_destroy (void *argument);

// ----------------------------------------------------------
typedef struct {
	dispatcher_t	*dispatcher;	// Dispatcher instance
    char			*hostName;		// Identity of worker
    zframe_t		*address;		// Address frame of worker, which assist dispatcher to route message to
    service_t		*service;		// Point to service_t

    heartbeat_t		*heartbeat;
} worker_t;

static void
	worker_delete (worker_t *self, int disconnect);
static void
	worker_destroy (void *argument);

// ----------------------------------------------------------
void _sendcmd(dispatcher_t* self, zframe_t *reply_to, int arg_length, ...)
{
//	if(!socket) return;
	if(!self) return;
	if(!self->socket) return;

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
	zmsg_pushstr(msg, DISPATCHER);
	zmsg_wrap(msg, zframe_dup(reply_to));

	zlog_debug(self->log, "Sending...");
	dumpzmsg(self->log, msg);

#if TASKPROC_IN_MULTITHREAD
	if(self->threads) {
		if(pthread_mutex_lock(&(self->sock_lock)) != 0) {
			zlog_error(self->log, "Unable to lock send_mutex in _sendcmd(), do not send message");
			return;
		}
	}
#endif
	if(zmsg_send(&msg, self->socket))
		zlog_error(self->log, "Failed to send in zmsg_send(), error code = %d", errno);
#if TASKPROC_IN_MULTITHREAD
	if(self->threads) {
		if(pthread_mutex_unlock(&(self->sock_lock)) != 0) {
			zlog_error(self->log,"Failed to unlock send_mutex in _sendcmd(), subsequent messages may be blocked");
			return;
		}
	}
#endif
}

//  Constructor of dispatcher
static dispatcher_t *
dispatcher_new()
{
	dispatcher_t *self = (dispatcher_t *) zmalloc (sizeof (dispatcher_t));
	zlog_init("log.conf");
	self->log = zlog_get_category("dispatcher");
	if(!self->log)
		printf("zlog_get_category() failed\n");
	zlog_info(self->log, "Creating dispatcher...");
    //  Initialize state
    self->ctx = zctx_new();
    self->socket = zsocket_new(self->ctx, ZMQ_ROUTER);
    self->services = zhash_new();
   	self->workers = zhash_new();
    self->tasks = zhash_new();
#if TASKPROC_IN_MULTITHREAD
    if(pthread_mutex_init(&(self->sock_lock), NULL) == 0)
    	self->threads = threadpool_create(6, 24, 0);
#endif
    return self;
}

// Destructor of dispatcher
static void
dispatcher_destroy(dispatcher_t **self_p)
{
    assert(self_p);
    if(*self_p) {
    	dispatcher_t *self = *self_p;
        zctx_destroy(&self->ctx);
        zhash_destroy(&self->services);
        zhash_destroy(&self->workers);
        zhash_destroy(&self->tasks);
#if TASKPROC_IN_MULTITHREAD
        if(self->threads) {
        	threadpool_destroy(self->threads, 0);
        	pthread_mutex_destroy(&(self->sock_lock));
        }
#endif
        zlog_fini();
        FREE(self);
        *self_p = NULL;
    }
}

static void
service_destroy(void *argument)
{
    service_t *service = (service_t *)argument;
    if(service) {
		// No need to do zlist_pop(service->tasks) and call task_destroy(&task)
		// because it will be destroyed in zhash_destroy(&service->tasks) in
		// dispatcher_destroy()
		zlist_destroy(&service->tasks);
		zlist_destroy(&service->workers);
		free(service->name);
		free(service);
    }
}

static service_t *
service_require(dispatcher_t *self, zframe_t *service_frame)
{
    if(!self || !service_frame) return NULL;

    char *name = zframe_strdup (service_frame);
    service_t *service = (service_t *)zhash_lookup(self->services, name);
    if(!service) {
    	zlog_info(self->log, "Adding service: %s", name);
        service = (service_t *)zmalloc(sizeof(service_t));
        service->dispatcher = self;
        service->name = name;
        service->workers = zlist_new();
        service->tasks = zlist_new();

        zhash_insert(self->services, name, service);
        zhash_freefn(self->services, name, service_destroy);
    }
    else {
    	zlog_info(self->log, "%s service already existed", name);
    	FREE(name);
    }
    return service;
}

static void
worker_delete(worker_t *worker, int disconnect)
{
    assert(worker);

    // Send DISCONNECT to worker if needed
    if(disconnect) {
    	zlog_info(worker->dispatcher->log, "Send DISCONNECT to worker [%s]", worker->hostName);
    	_sendcmd(worker->dispatcher, worker->address, 1, cmd_code2payload(DISCONNECT));
    }

    // Remove worker from on-duty list of self->service
    if (worker->service) {
        zlist_remove(worker->service->workers, worker);
    }

    // Remove worker from dispatcher's worker hash table
    zhash_delete(worker->dispatcher->workers, worker->hostName); // This implicitly calls worker_destroy
}

// Worker destructor is called automatically whenever the worker is
// removed from dispatcher->workers (called by zhash_delete())
static void
worker_destroy(void *argument)
{
    worker_t *worker = (worker_t *)argument;
    if(worker) {
    	if(worker->hostName) {
			zlog_info(worker->dispatcher->log, "Remove worker [%s]", worker->hostName);
			free(worker->hostName);
		}
    	if(worker->address)	zframe_destroy(&worker->address);
    	heartbeat_destroy(&worker->heartbeat);
    	free(worker);
    }
}

static void
process_msg_from_client (dispatcher_t *self, zframe_t *sender, zmsg_t *msg)
{
	// $sender destroy is controlled by caller, don't handle it.
	// $msg must be destroyed in this method

    assert(zmsg_size (msg) >= 1);     //  At least, command

    // Get command in payload format
    char *command = zmsg_popstr(msg);

    switch(cmd_payload2code(command))
    {
    case TASKCREATEREQ:
    	if(is_zmsg_size_enough(self->log, msg, 4)) {
			char *token = zmsg_popstr(msg);
			char *workers = zmsg_popstr(msg);
			char *method = zmsg_popstr(msg);
			char *data = zmsg_popstr(msg);

			// Check this task is to service group or specific worker
			char *serviceName;
			zframe_t *worker = NULL;
			char *delimiter = strchr(workers, '.');
			// To specific worker
			if((delimiter != NULL) && (*(delimiter + 1) != '\0')) {
				worker = zframe_new(workers, strlen(workers));
				int serviceName_length = delimiter - workers;
				serviceName = (char *)zmalloc(sizeof(char) * serviceName_length + 1);
				strncpy(serviceName, workers, serviceName_length);
				serviceName[serviceName_length] = '\0';
				zlog_info(self->log, "Receive TASKCREATEREQ, to specific worker [%s], service type [%s]", workers, serviceName);
			}
			// Service only
			else {
				serviceName = strdup(workers);
				zlog_info(self->log, "Receive TASKCREATEREQ, to service type [%s] only", serviceName);
			}

			service_t *service = (service_t *)zhash_lookup(self->services, serviceName);
			// No service registered, reply S_UNKNOWNSERV to client
			if(!service) {
				zlog_info(self->log, "No service type [%s] registered, reply S_UNKNOWNSERV to token [%s]", serviceName, token);
				_sendcmd(self, sender, 4, cmd_code2payload(TASKCREATEREP), token, NULL_UUID, stat_code2payload(S_UNKNOWNSERV));
			}
			// Found service
			else {
				// Create task ID, create task item to hash table and append to
				// service->tasks for future service_dispatch()
				task_t *task = task_create(serviceName, sender, method, data, worker); // If has specific worker, assigned here. If no, assigned by service_dispatch()
				zhash_insert (self->tasks, task_get_taskID(task), task);
				zhash_freefn (self->tasks, task_get_taskID(task), task_destroy);
				zlist_append (service->tasks, task); // For service_dispatch()

				zlog_debug(self->log, "Create task:");
				zlog_debug(self->log, "  taskID = %s", task_get_taskID(task) ? task_get_taskID(task) : "NULL");
				zlog_debug(self->log, "  serviceName = %s", task_get_servicename(task) ? task_get_servicename(task) : "NULL");
				zlog_debug(self->log, "  client = %s", task_get_clientstr(task) ? task_get_clientstr(task) : "NULL");
				zlog_debug(self->log, "  method = %s", task_get_method(task) ? task_get_method(task) : "NULL");
				zlog_debug(self->log, "  data = %s", task_get_data(task) ? task_get_data(task) : "NULL");
				zlog_debug(self->log, "  worker = %s", task_get_workerstr(task) ? task_get_workerstr(task) : "NULL");

				// Return task ID to client
				zlog_info(self->log, "Reply TASKCREATEREP with task ID [%s] to client", task_get_taskID(task));
				_sendcmd(self, sender, 4, cmd_code2payload(TASKCREATEREP), token, task_get_taskID(task), stat_code2payload(S_OK));
			}

			if(worker) zframe_destroy(&worker);
			FREE(serviceName);

			FREE(token);
			FREE(workers);
			FREE(method);
			FREE(data);
		}
		break;

    case TASKQUERYREQ:
    	if(is_zmsg_size_enough(self->log, msg, 1)) {
    		// Search from task DB and return percentage
    		char *taskid = zmsg_popstr(msg);

    		char *status; // can't be freed because it just pass stat_code2payload returned string, that is const
    		// taskid can't be NULL_UUID, return FAIL 904 (not S_FAIL 400)
    		if(!strcmp(taskid, NULL_UUID)) {
    			zlog_info(self->log, "Receive TASKQUERYREQ with NULL UUID, return FAIL to client");
    			status = stat_code2payload(FAIL);
    		}
    		else {
    			task_t *task = zhash_lookup(self->tasks, taskid);
				// Task found, reply percentage (could be percentage, DISPATCHING or FAIL
    			if(task) {
    				zlog_info(self->log, "Receive TASKQUERYREQ with task ID [%s] and found, return %3d%% to client", taskid, task_get_status(task));
    				status = uitoa(task_get_status(task));
    			}
    			// Task not found, reply S_NOTFOUND
    			else {
    				zlog_info(self->log, "Receive TASKQUERYREQ with task ID [%s] but not found, return FAIL to client", taskid);
    				status = stat_code2payload(S_NOTFOUND);
    			}
				// TODO: after query, if task status percentage is 100, remove it from self->tasks?
    		}
			_sendcmd(self, sender, 3, cmd_code2payload(TASKQUERYREP), taskid, status);
			FREE(status);

			FREE(taskid);
    	}
    	break;

    // opshort, received from client and forward to worker
    case TASKDIRECTREQ:
    	if(is_zmsg_size_enough(self->log, msg, 4)) {
			char *token = zmsg_popstr(msg);
			char *workers = zmsg_popstr(msg);
			char *method = zmsg_popstr(msg);
			char *data = zmsg_popstr(msg);

			// Check this task is to service group or specific worker
			char *serviceName;
			char *delimiter = strchr(workers, '.');
			worker_t *worker = NULL;
			// To specific worker
			if((delimiter != NULL) && (*(delimiter + 1) != '\0')) {
				int serviceName_length = delimiter - workers;
				serviceName = (char *)zmalloc(sizeof(char) * serviceName_length);
				strncpy(serviceName, workers, serviceName_length);
				serviceName[serviceName_length] = '\0';
				zlog_info(self->log, "Receive TASKDIRECTREQ, to specific worker [%s], service type [%s]", workers, serviceName);

				// Found specific worker
				worker = (worker_t *)zhash_lookup(self->workers, workers);
				if(worker) {
					zlog_info(self->log, "Worker [%s] found, forward TASKDIRECTREQ to it", workers);
					zlist_remove(worker->service->workers, worker);
				}
				// Can't find specific worker, reply client empty frame as error to client
				else {
					zlog_info(self->log, "Can't find worker [%s], reply empty frame as error to client", workers);
					_sendcmd(self, sender,
							3, cmd_code2payload(TASKDIRECTREP), token, "");
				}
			}
			// Service only
			else {
				serviceName = strdup(workers);
				zlog_info(self->log, "Receive TASKDIRECTREQ, to service type [%s] only", serviceName);

				service_t *service = (service_t *)zhash_lookup(self->services, serviceName);
				// Found service
				if(service) {
					// But no on-duty worker, reply empty frame as error to client
					if(zlist_size (service->workers) == 0) {
						zlog_info(self->log, "Service type [%s] registered but no worker on-duty, reply empty frame as error to client", serviceName);
						_sendcmd(self, sender,
								3, cmd_code2payload(TASKDIRECTREP), token, "");
					}
					else {
						worker = zlist_pop(service->workers);
						zlog_info(self->log, "Service type [%s] registered and found available worker [%s], forward TASKDIRECTREQ to it", serviceName, worker->hostName);
					}
				}
				// No service registered, reply empty frame as error to client
				else {
					zlog_info(self->log, "No service type [%s] registered, reply empty frame as error to client");
					_sendcmd(self, sender,
							3, cmd_code2payload(TASKDIRECTREP), token, "");
				}
				FREE(serviceName);
			}
			if(worker) {
				char *client = zframe_strdup(sender);
				zlog_debug(self->log, "Forwarding TASKDIRECTREQ to worker [%s]...", worker->hostName);
				zlog_debug(self->log, "  token = %s", token);
				zlog_debug(self->log, "  client = %s", client);
				zlog_debug(self->log, "  method = %s", method);
				zlog_debug(self->log, "  data = %s", data);
				_sendcmd(self, worker->address, 5, cmd_code2payload(TASKDIRECTREQ), token, client, method, data);
				FREE(client);
				// Workers are scheduled in the round-robin fashion
				zlist_append(worker->service->workers, worker); // push back worker to the last one
			}

			FREE(token);
			FREE(workers);
			FREE(method);
			FREE(data);
    	}
    	break;

    case UNKNOWN:
    default:
    	{
			char *client = zframe_strdup(sender);
			if(client) {
				zlog_info(self->log, "Unknown command [%s] from client [%s], drop it silently", command, client);
				free(client);
			}
    	}
		break;
    }
    FREE(command);
    zmsg_destroy(&msg);
}

void
_heartbeat_sendfn(void *param)
{
	worker_t *self = param;
	if(self) {
		_sendcmd(self->dispatcher, self->address, 1, cmd_code2payload(HEARTBEAT));
	}
}

static void
process_msg_from_worker (dispatcher_t *self, zframe_t *sender, zmsg_t *msg)
{
    assert (zmsg_size (msg) >= 1);     //  At least, command

    // 1. Get command in payload format
    char *command = zmsg_popstr(msg);

    // 2. Lookup worker exists or not by identity
    char *hostName = zframe_strdup(sender);
    worker_t *worker = zhash_lookup(self->workers, hostName);
    int worker_exists = (worker) ? 1 : 0;

    switch(cmd_payload2code(command))
    {
    // Got service registration request
    case SERVICEREGREQ:
    	if (worker_exists) {
    		// SERVICEREGREQ is for newly created session but worker already registered.
    		// Case 1: This is an corrupted session, disconnect worker and remove it.
    		zlog_info(self->log, "Receive SERVICEREGREQ from already existed worker [%s], disconnect it due to session corruption", hostName);
    		_sendcmd(self, worker->address, 2, cmd_code2payload(SERVICEREGREP), stat_code2payload(S_EXISTED));
    		worker_delete(worker, 1);
    		// TODO: what will happen if two nodes to register the same name??
    		// TODO: Case 2: If we have authorization phase, should try to authorize again
    	}
		else {
			if(is_zmsg_size_enough(self->log, msg, 1)) {
				//  Create worker for the service type and put to on-duty list
				worker = (worker_t *)zmalloc(sizeof (worker_t));
				worker->dispatcher = self;
				worker->hostName = strdup(hostName);
				zframe_t *service_frame = zmsg_pop(msg); // fetch service name
				worker->service = service_require(self, service_frame);
				zframe_destroy(&service_frame);
				worker->address = zframe_dup(sender);
				worker->heartbeat = heartbeat_create(HEARTBEAT_LIVEING, HEARTBEAT_INTERVAL, _heartbeat_sendfn, (void *)worker);
				zhash_insert(self->workers, hostName, worker);
				zhash_freefn(self->workers, hostName, worker_destroy);
				zlist_append(worker->service->workers, worker);
				_sendcmd(self, worker->address,	2, cmd_code2payload(SERVICEREGREP), stat_code2payload(S_OK)); // reply worker registration success
				zlog_info(self->log, "Receive SERVICEREGREQ from worker [%s] and register service type [%s], register success", worker->hostName, worker->service->name);
			}
		}
    	break;

    // Got oplong task acceptance
    case TASKDISREP:
    	if(worker_exists && is_zmsg_size_enough(self->log, msg, 2)) {
    		// Update task status in task DB
    		char *taskid = zmsg_popstr(msg);
    		char *code = zmsg_popstr(msg);

			task_t *task = zhash_lookup(self->tasks, taskid);
			if(task) {
				// Task found, update percentage & modifyTime
				if(stat_payload2code(code) == S_OK) {
					zlog_info(self->log, "Receive TASKDISREP [S_OK] for task ID [%s] from worker [%s], update status to 0", taskid, hostName);
					task_set_status(task, 0); // receive S_OK
				}
				else {
					zlog_info(self->log, "Receive TASKDISREP [S_FAIL] for task ID [%s] from worker [%s], update status to FAIL", taskid, hostName);
					task_set_status(task, FAIL); // receive S_FAIL
				}
			}
			else {
				// Task not-found, discard it silently
				zlog_info(self->log, "Receive TASKDISREP for task ID [%s] from worker [%s] but not found, do nothing", taskid, hostName);
			}

			FREE(taskid);
			FREE(code);
    	}
    	break;

    // Receive update for oplong task
    case TASKUPDATE:
    	if(worker_exists && is_zmsg_size_enough(self->log, msg, 2)) {
			// Search task DB and update percentage
			char *taskid = zmsg_popstr(msg);
			char *percentage = zmsg_popstr(msg);

			unsigned int per = atoi(percentage);
			if(per > 100 && per != FAIL) per = 100;

			task_t *task = zhash_lookup(self->tasks, taskid);
			// Task found, update percentage & modifyTime
			if(task) {
				zlog_info(self->log, "Receive TASKUPDATE [%3u%%] for task ID [%s] from worker [%s], update it", per, taskid, hostName);
				task_set_status(task, per);
			}
			// Task not-found
			else {
				zlog_info(self->log, "Receive TASKUPDATE [%3u%%] for task ID [%s] from worker [%s] but not found, do nothing", per, taskid, hostName);
			}

			FREE(taskid);
			FREE(percentage);
		}
    	break;

    // opshort, received from worker and forward to client
    case TASKDIRECTREP:
    	if(worker_exists && is_zmsg_size_enough(self->log, msg, 3)) {
    		char *token = zmsg_popstr(msg);
    		zframe_t *client_frame = zmsg_pop(msg);
    		char *data = zmsg_popstr(msg);

    		zlog_info(self->log, "Receive TASKDIRECTREP for token [%s] from worker [%s], forward to client", token, hostName);
    		_sendcmd(self, client_frame, 3, cmd_code2payload(TASKDIRECTREP), token, data);

    		FREE(token);
    		zframe_destroy(&client_frame);
    		FREE(data);
    	}
    	break;

    case HEARTBEAT:
    	// Worker exists, reset heartbeat retries
    	if(worker_exists)
    		heartbeat_reset_retries(worker->heartbeat);
//    	// Worker doesn't exist but send HEARTBEAT, disconnect it
//    	else
//    		worker_delete(worker, 1);
    	break;

    case DISCONNECT:
    	if(worker_exists) {
			zlog_info(self->log, "Receive DISCONNECT request from worker [%s], remove it", hostName);
			worker_delete(worker, 0);
    	}
    	break;

    case UNKNOWN:
    default:
    	zlog_info(self->log, "Receive unknown command [%s] from worker [%s]", command, hostName);
    	break;
    }
    FREE(hostName);
    FREE(command);
    zmsg_destroy (&msg);
}

#define TASK_TIMEOUT_IN_MS	10000

static void
service_dispatch(void *ptr)
{
	if(!ptr) return;

	service_t *self = (service_t *)ptr;

	// Remove dead workers and do heartbeat before checking task
	// size. We need to send heartbeat even there is no task.
	for(worker_t *worker = (worker_t *)zlist_first(self->workers);
			worker != NULL;
			worker = (worker_t *)zlist_next(self->workers)) {
		if(heartbeat_check(worker->heartbeat) == 0) {
			zlog_info(self->dispatcher->log, "Worker [%s] was dead, delete it", worker->hostName);
			worker_delete(worker, 1);
		}
	}

    int count = zlist_size(self->tasks);
    if(count > 0)
    	zlog_debug(self->dispatcher->log, "[%s] service dispatching:", self->name);
    else
    	return;

    while(count-- > 0) {
        task_t *task = (task_t *)zlist_pop(self->tasks);

        // Task was done (i.e. dispatched is true and status is 100/FAIL), we don't need
        // to take care of it anymore, remove it by not pushing back to task list
        if(task_get_dispatched(task) && ((task_get_status(task) == 100) || (task_get_status(task) == FAIL)))
        	continue;

        // If task is timeout'd, remove it by not pushing back to task list and change status to FAIL
        if(task_get_expired(task)) {
        	zlog_info(self->dispatcher->log, "Task [%s] was timeout'd, change status to FAIL and won't be dispatch anymore", task_get_taskID(task));
        	task_set_status(task, FAIL);
			continue;
        }

        // If task wasn't done and not timeout'd, check if the task
        // is dispatched or not. If not, try to dispatch it
		if(!task_get_dispatched(task)) {
			worker_t *worker = NULL;
			// To specific worker
			if(task_get_worker(task)) {
				worker = zhash_lookup(self->dispatcher->workers, task_get_workerstr(task));
				// Can't find specific worker, push back to the end of task list
				if(!worker) {
					zlog_debug(self->dispatcher->log, "Task [%s] specify worker [%s], but can't found", task_get_taskID(task), task_get_workerstr(task));
				}
				// Find specific worker, go on
				else {
					zlist_remove(self->workers, worker); // remove worker first, will append back after dispatching
				}
			}
			// To service
			else {
				// pop up the first worker to process task
				worker = (worker_t*)zlist_pop(self->workers);
				task_set_worker(task, worker->address);
			}

			if(worker) {
				zlog_info(self->dispatcher->log, "Dispatching task [%s] to worker [%s]", task_get_taskID(task), worker->hostName);
				_sendcmd(self->dispatcher, worker->address,
						4, cmd_code2payload(TASKDISREQ), task_get_taskID(task), task_get_method(task), task_get_data(task));
				task_set_dispatched(task, 1);

				// Workers are scheduled in the round-robin fashion
				zlist_append(self->workers, worker); // push worker back to on-duty list
			}
		}
        zlist_append(self->tasks, task); // push task back to the end of task list
    }
}

int service_dispatch_all(const char *key, void *item, void *argument)
{
	service_t *service = item;
	if(service) {
#if TASKPROC_IN_MULTITHREAD
			if(service->dispatcher->threads) {
				if(threadpool_add(service->dispatcher->threads, &service_dispatch, service, 0) == 0)
					zlog_debug(service->dispatcher->log, "Dispatch [%s] service by threadpool", service->name);
				else {
					zlog_debug(service->dispatcher->log, "Dispatch [%s] service by current thread due to no thread available from threadpool", service->name);
					service_dispatch(service);
				}
			}
			else
#endif
				service_dispatch(service);
	}
	return 0;
}

int disconnect_all_workers(const char *key, void *item, void *argument)
{
	worker_t *worker = item;
	if(worker) {
		worker_delete(worker, 1);
	}
	return 0;
}

void test_serialization()
{
	zframe_t *a = zframe_new("123", 3);
	zframe_t *b = zframe_new("456", 3);
	task_t *task1 = NULL, *task2 = NULL;

	task1 = task_create("serviceName", a, "method", "data", b);

	if(task1) {
		printf("task1:\n");
		task_print(task1);
	}

	serialize_t *buf = serialize_create();
	if(buf) {
		if(task_serialize(task1, buf) == 0) {
			char *item = serialize_bufdup(buf);
			if(item) {
				//printf("Buf[%d]: %s\n", (int)strlen(item) + 1, item);
				serialize_reset(buf);
				serialize_bufset(buf, item);
				task2 = task_deserialize(buf);
				free(item);
			}
		}
		serialize_destroy(buf);
	}

	if(task2) {
		printf("task2:\n");
		task_print(task2);
	}

	if(task1) task_destroy(task1);
	if(task2) task_destroy(task2);

	zframe_destroy(&a);
	zframe_destroy(&b);
}

int main(int argc, char **argv)
{
//	test_serialization();

	if(argc != 2) {
		printf("Usage: %s tcp://*:<port>\n", argv[0]);
		return 1;
	}

	// TODO: fail recovery, to read previous state here or in dispatcher_new()

	dispatcher_t *self = dispatcher_new();
	if(!self) return 1;
	zsocket_bind(self->socket, argv[1]);
	zlog_info(self->log, "Bind to %s", argv[1]);

	int quit = 0;
	// Get and process messages forever or until interrupted
	while(!quit) {
		zmq_pollitem_t items [] = {
			{ self->socket,  0, ZMQ_POLLIN, 0 } };
		int rc = zmq_poll (items, 1, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
		if (rc == -1) break; //  Interrupted -> zctx_interrupted

		//  Process next input message, if any
		if (items [0].revents & ZMQ_POLLIN) {
#if TASKPROC_IN_MULTITHREAD
			if(self->threads) {
				if(pthread_mutex_lock(&(self->sock_lock)) != 0) {
					zlog_error(self->log, "Unable to lock mutex for zmsg_recv(), do not receive message");
					continue;
				}
			}
#endif
			zmsg_t *msg = zmsg_recv(self->socket);
#if TASKPROC_IN_MULTITHREAD
			if(self->threads) {
				if(pthread_mutex_unlock(&(self->sock_lock)) != 0) {
					zlog_error(self->log,"Failed to unlock mutex for zmsg_recv(), terminated...");
					quit = 1;
				}
			}
#endif
			if (!msg) break; //  Interrupted -> zctx_interrupted

			zlog_debug(self->log, "Receive message");
			dumpzmsg(self->log, msg);

			zframe_t *sender = zmsg_pop (msg);
			zframe_t *empty  = zmsg_pop (msg);
			zframe_t *role = zmsg_pop (msg);

			if(zframe_streq (role, CLIENT)) {
				zlog_debug(self->log, "Receive message from client");
				process_msg_from_client(self, sender, msg); // process_msg_from_client will destroy $msg
			}
			else if(zframe_streq (role, WORKER)) {
				zlog_debug(self->log, "Receive message from worker");
				process_msg_from_worker(self, sender, msg); // process_msg_from_worker will destroy $msg
			}
			else {
				zlog_debug(self->log, "Invalid message");
				zmsg_destroy (&msg);
			}

			zframe_destroy (&sender);
			zframe_destroy (&empty);
			zframe_destroy (&role);
		}

		zhash_foreach(self->services, &service_dispatch_all, NULL); // heartbeat is also manipulated in service_distpach
#if TASKPROC_IN_MULTITHREAD
		if(self->threads) {
			while(threadpool_task_size(self->threads) > 0)
				zclock_sleep(100);
		}
#endif
	}
	if (zctx_interrupted) {
		zlog_info(self->log, "Interrupt received, shutting down...");
		zhash_foreach(self->workers, &disconnect_all_workers, NULL); // Send DISCONNECT to all connected workers

		// TODO: backup database to persistence storage
	}

	dispatcher_destroy(&self);
	return 0;
}

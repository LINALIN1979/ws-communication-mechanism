// TODO: secure channel
// TODO: integrate authentication, authorization
// TODO: serialization

#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option

#define TASKPROC_IN_MULTITHREAD	// Toggle of multi-thread supprt
#define SAVE_STATE_TO_DATABASE	// Toggle of save state to database

#include "../wscommon/protocol.h"
#include "../wscommon/utils.h"
#include "task.h"
#ifdef SAVE_STATE_TO_DATABASE
	#include <postgresql/libpq-fe.h>
#endif


#define HEARTBEAT_LIVEING	15000	// If doesn't receive peer heartbeat exceeds this period, will treat as dead
#define HEARTBEAT_INTERVAL  3000	// Heartbeat sending interval (in millisecond)

// ==========================================================
typedef struct {
	zctx_t			*ctx;			// Context
	char			*bind;			// Dispatcher bind address
	void			*socket;		// Socket for clients & workers to connect

#ifdef SAVE_STATE_TO_DATABASE
	PGconn			*dbconn;
 #if defined TASKPROC_IN_MULTITHREAD
	pthread_mutex_t db_write_lock;
 #endif
#endif

	zhash_t			*workers;		// Hash table to store connected workers
	zhash_t			*services;		// Hash of known services
	zhash_t			*tasks;			// Hash of tasks

	zlog_category_t	*log;

#ifdef TASKPROC_IN_MULTITHREAD
	threadpool_t	*threads;
	pthread_mutex_t	sock_lock;
#endif
} dispatcher_t;

static dispatcher_t*	dispatcher_new();
static void				dispatcher_destroy(dispatcher_t **self_p);

// ----------------------------------------------------------
typedef struct {
	dispatcher_t	*dispatcher;	// Dispatcher instance
    char			*name;			// Service name
    zlist_t			*workers;		// Queue of on-duty workers

    zlist_t			*tasks;			// List of ongoing tasks (status != 100 or FAIL)
} service_t;

static service_t*		service_require(dispatcher_t *self, char *service_frame);
static void				service_destroy(void *argument);

// ----------------------------------------------------------
typedef struct {
	dispatcher_t	*dispatcher;	// Dispatcher instance
    char			*hostName;		// Identity of worker
    zframe_t		*address;		// Address frame of worker, which assist dispatcher to route message to
    service_t		*service;		// Point to service_t

    heartbeat_t		*heartbeat;
} worker_t;

static void				worker_delete(worker_t *self, int disconnect);
static void				worker_destroy(void *argument);

// ==========================================================
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

#ifdef TASKPROC_IN_MULTITHREAD
	if(self->threads) {
		if(pthread_mutex_lock(&(self->sock_lock)) != 0) {
			zlog_error(self->log, "Unable to lock send_mutex in _sendcmd(), do not send message");
			return;
		}
	}
#endif
	if(zmsg_send(&msg, self->socket))
		zlog_error(self->log, "Failed to send in zmsg_send(), error code = %d", errno);
#ifdef TASKPROC_IN_MULTITHREAD
	if(self->threads) {
		if(pthread_mutex_unlock(&(self->sock_lock)) != 0) {
			zlog_error(self->log,"Failed to unlock send_mutex in _sendcmd(), subsequent messages may be blocked");
			return;
		}
	}
#endif
}

void
_heartbeat_sendfn(void *param)
{
	worker_t *self = param;
	if(self) {
		_sendcmd(self->dispatcher, self->address, 1, cmd_code2payload(HEARTBEAT));
	}
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

#ifdef SAVE_STATE_TO_DATABASE
 #if defined TASKPROC_IN_MULTITHREAD
	pthread_mutex_init(&(self->db_write_lock), NULL);
 #endif
    // FIXME: [DB] ========================================
    self->dbconn = PQconnectdb("dbname=testdb user=test password=test");
    if(PQstatus(self->dbconn) == CONNECTION_OK) {
    	zlog_debug(self->log, "Rebuild services/workers/tasks from database");

    	PGresult *services, *workers, *tasks;

    	// rebuild self->services
    	char *sql_cmd;
    	sql_cmd = "SELECT * FROM dispatcher.services";
    	services = PQexec(self->dbconn, sql_cmd);
    	if(PQresultStatus(services) == PGRES_TUPLES_OK) {
    		int name_fnum = PQfnumber(services, "name");
    		for(int i = 0; i < PQntuples(services); i++) {
    			// create service_t to store and put into self->services
    			char *name = PQgetvalue(services, i, name_fnum);
    			const char *params[1] = { name };
    			service_t *service = (service_t *)zmalloc(sizeof(service_t));
				service->dispatcher = self;
				service->name = strdup(name);
				service->workers = zlist_new();
				service->tasks = zlist_new();
				zhash_insert(self->services, name, service);
				zhash_freefn(self->services, name, service_destroy);
				zlog_debug(self->log, "  Create service [%s]", name);

    			// search workers belong to this service
    			sql_cmd = "SELECT * FROM dispatcher.workers WHERE service_name=$1";
				workers = PQexecParams(self->dbconn,
						sql_cmd,
						1, 		// one param
						NULL,	// let the backend deduce param type
						params,
						NULL,	// don't need param lengths since text
						NULL,	// default to all text params
						1);
				if(PQresultStatus(workers) == PGRES_TUPLES_OK) {
					int name_fnum				 = PQfnumber(workers, "name");
					int heartbeat_deadtime_fnum	 = PQfnumber(workers, "heartbeat_deadtime");
					int heartbeat_keepalive_fnum = PQfnumber(workers, "heartbeat_keepalive");
					int timeout_old_fnum		 = PQfnumber(workers, "timeout_old");
					int timeout_new_fnum		 = PQfnumber(workers, "timeout_new");
					int timeout_interval_fnum	 = PQfnumber(workers, "timeout_interval");
					int retries_fnum			 = PQfnumber(workers, "retries");
					for(int i = 0; i < PQntuples(workers); i++) {
						// create workre_t to store and put into
						// 1. self->workers
						// 2. self->services->workers
						worker_t *worker = (worker_t *)zmalloc(sizeof (worker_t));
						worker->dispatcher = self;
						worker->hostName = strdup(PQgetvalue(workers, i, name_fnum));
						worker->service = service;
						worker->address = zframe_new(worker->hostName, strlen(worker->hostName));
						char *tmp = PQgetvalue(workers, i, heartbeat_deadtime_fnum);
						uint64_t heartbeat_deadtime = atoul(tmp, strlen(tmp), 10);
						//printf("deadtime = %lu\n", heartbeat_deadtime);
						tmp = PQgetvalue(workers, i, heartbeat_keepalive_fnum);
						uint64_t heartbeat_keepalive = atoul(tmp, strlen(tmp), 10);
						//printf("keepalive = %lu\n", heartbeat_keepalive);
						tmp = PQgetvalue(workers, i, timeout_old_fnum);
						uint64_t timeout_old = atoul(tmp, strlen(tmp), 10);
						//printf("timeout_old = %lu\n", timeout_old);
						tmp = PQgetvalue(workers, i, timeout_new_fnum);
						uint64_t timeout_new = atoul(tmp, strlen(tmp), 10);
						//printf("timeout_new = %lu\n", timeout_new);
						tmp = PQgetvalue(workers, i, timeout_interval_fnum);
						uint64_t timeout_interval = atoul(tmp, strlen(tmp), 10);
						//printf("timeout_interval = %lu\n", timeout_interval);
						tmp = PQgetvalue(workers, i, retries_fnum);
						int retries = atoi(tmp);
						//printf("retries = %d\n", retries);
						worker->heartbeat = heartbeat_create_manually(heartbeat_deadtime, heartbeat_keepalive,
								timeout_old, timeout_new, timeout_interval, retries,
								_heartbeat_sendfn, (void *)worker);
						zhash_insert(self->workers, worker->hostName, worker);
						zhash_freefn(self->workers, worker->hostName, worker_destroy);
						zlist_append(worker->service->workers, worker);
						zlog_debug(self->log, "  Add worker [%s] to service [%s]", worker->hostName, service->name);
					}
				}
				else {
					zlog_error(self->log, "  Failed in SQL command: %s", sql_cmd);
					zlog_error(self->log, "  %s", PQerrorMessage(self->dbconn));
				}
				PQclear(workers);

				// search tasks belong to this service
				sql_cmd = "SELECT * FROM dispatcher.tasks WHERE service_name=$1";
				tasks = PQexecParams(self->dbconn,
						sql_cmd,
						1, 		// one param
						NULL,	// let the backend deduce param type
						params,
						NULL,	// don't need param lengths since param type is text
						NULL,	// default to all text params
						1);
				if(PQresultStatus(tasks) == PGRES_TUPLES_OK) {
					int taskID_fnum				 = PQfnumber(tasks, "taskID");
					int status_fnum				 = PQfnumber(tasks, "status");
					int dispatched_fnum			 = PQfnumber(tasks, "dispatched");
					int create_time_fnum		 = PQfnumber(tasks, "create_time");
					int timeout_old_fnum		 = PQfnumber(tasks, "timeout_old");
					int timeout_new_fnum		 = PQfnumber(tasks, "timeout_new");
					int timeout_interval_fnum	 = PQfnumber(tasks, "timeout_interval");
					int method_fnum				 = PQfnumber(tasks, "method");
					int data_fnum				 = PQfnumber(tasks, "data");
					int client_str_fnum			 = PQfnumber(tasks, "client_str");
					int worker_str_fnum			 = PQfnumber(tasks, "worker_str");
					for(int i = 0; i < PQntuples(tasks); i++) {
						// create task_t to store and put into
						// 1. self->tasks: maybe recover all task at once later
						// 2. self->services->tasks
						char *tmp;
						tmp = PQgetvalue(tasks, i, status_fnum);
						unsigned int status = atoi(tmp);
						//printf("         status = %u\n", status);
						tmp = PQgetvalue(tasks, i, dispatched_fnum);
						int dispatched = atoi(tmp);
						//printf("         dispatched = %d\n", dispatched);
						tmp = PQgetvalue(workers, i, timeout_old_fnum);
						uint64_t timeout_old = atoul(tmp, strlen(tmp), 10);
						tmp = PQgetvalue(workers, i, timeout_new_fnum);
						uint64_t timeout_new = atoul(tmp, strlen(tmp), 10);
						tmp = PQgetvalue(workers, i, timeout_interval_fnum);
						uint64_t timeout_interval = atoul(tmp, strlen(tmp), 10);
						tmp = PQgetvalue(workers, i, create_time_fnum);
						uint64_t createTime = atoul(tmp, strlen(tmp), 10);
						task_t *task = task_create_manually(PQgetvalue(tasks, i, taskID_fnum), status, dispatched,
								createTime, timeout_old, timeout_new, timeout_interval,
								service->name, PQgetvalue(tasks, i, method_fnum), PQgetvalue(tasks, i, data_fnum),
								PQgetvalue(tasks, i, client_str_fnum), PQgetvalue(tasks, i, worker_str_fnum));
						zhash_insert(self->tasks, task_get_taskID(task), task);
						zhash_freefn(self->tasks, task_get_taskID(task), task_destroy);

						if(!task_get_dispatched(task) || // Task waits for dispatching
							!((task_get_status(task) == 100) || (task_get_status(task) == FAIL))) { // Task dispatched but not done/failed
							zlist_append(service->tasks, task); // Task not finished, need to take care of it
							zlog_debug(self->log, "  Add task [%s] to dispatcher->tasks[%s] and wait for dispatch", task_get_taskID(task), service->name);
						}
						else {
							zlog_debug(self->log, "  Add task [%s] to dispatcher->tasks[%s] only", task_get_taskID(task), service->name);
						}
					}
				}
				else {
				    zlog_error(self->log, "  Failed in SQL command: %s", sql_cmd);
				    zlog_error(self->log, "  %s", PQerrorMessage(self->dbconn));
				}
				PQclear(tasks);
    		}
    	}
    	else {
    		zlog_error(self->log, "  Failed in SQL command: %s", sql_cmd);
    		zlog_error(self->log, "  %s", PQerrorMessage(self->dbconn));
    	}
    	PQclear(services);

    	// TODO: should we read out all the tasks that do not belong to any service?

    	// TODO: service_dispatch_all right now?
    }
    else {
    	zlog_error(self->log, "Connection to database failed: %s", PQerrorMessage(self->dbconn));
    }
    // [DB] ========================================
#endif
#ifdef TASKPROC_IN_MULTITHREAD
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
        if(self->ctx)		zctx_destroy(&self->ctx);
        if(self->services)	zhash_destroy(&self->services);
        if(self->workers)	zhash_destroy(&self->workers);
        if(self->tasks)		zhash_destroy(&self->tasks);
#ifdef SAVE_STATE_TO_DATABASE
 #if defined TASKPROC_IN_MULTITHREAD
        pthread_mutex_destroy(&(self->db_write_lock));
 #endif
        PQfinish(self->dbconn);
#endif
#ifdef TASKPROC_IN_MULTITHREAD
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

#ifdef SAVE_STATE_TO_DATABASE
int
DBExecCmd(PGconn *dbconn, pthread_mutex_t *lock, const char *cmd, ...)
{
	int ret = 0;
	if(dbconn) {
		// mutex_lock
		if(lock) {
			if(pthread_mutex_lock(lock) != 0) goto write_DB_return;
		}

		static char buf[1024];
		int go_on_exec = 0;

		va_list args;
		va_start(args, cmd);
		if(vsprintf(buf, cmd, args) > 0) go_on_exec = 1; // success
		va_end(args);

		//printf("write_DB: %s\n", buf);

		if(go_on_exec) {
			PGresult *op = PQexec(dbconn, buf);
			if(PQresultStatus(op) == PGRES_COMMAND_OK)
				ret = 1;
		}

		// mutex_unlock
		if(lock) pthread_mutex_unlock(lock);
	}
write_DB_return:
	return ret;
}
#endif

static service_t *
service_require(dispatcher_t *self, char *service_name)
{
    if(!self || !service_name) return NULL;

    char *name = strdup(service_name);
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

#ifdef SAVE_STATE_TO_DATABASE
        // FIXME: [DB] ========================================
//		const char *param[1];
//		param[0] = service_name;
//		PGresult *op = PQexecParams(self->dbconn,
//				"INSERT INTO dispatcher.services (name) VALUES ($1)",
//				1,		// int nParams
//				NULL,	// Oid *paramTypes, let the backend deduce param type
//				param,	// char * const *paramValues,
//				NULL,	// int *paramLengths, don't need param lengths since param type is text
//				NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//				1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//		if(PQresultStatus(op) == PGRES_COMMAND_OK)
//			zlog_debug(self->log, "Insert service [%s] to DB dispatcher.services success", service_name);
//		else
//			zlog_error(self->log, "Insert service [%s] to DB dispatcher.services failed: %s", service_name, PQerrorMessage(self->dbconn));
 #if defined TASKPROC_IN_MULTITHREAD
		if(DBExecCmd(self->dbconn, &self->db_write_lock,
 #else
		if(DBExecCmd(self->dbconn, NULL,
 #endif
				"INSERT INTO dispatcher.services (name) VALUES ('%s')", service_name))
			zlog_debug(self->log, "Insert service [%s] to DB dispatcher.services success", service_name);
		else
			zlog_error(self->log, "Insert service [%s] to DB dispatcher.services failed: %s", service_name, PQerrorMessage(self->dbconn));
		// [DB] ========================================
#endif
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

#ifdef SAVE_STATE_TO_DATABASE
    // [DB] remove worker from database
	// [DB] ========================================
//	const char *param[1];
//	param[0] = worker->hostName;
//	PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//			"DELETE FROM dispatcher.workers WHERE name=$1",
//			1,		// int nParams
//			NULL,	// Oid *paramTypes, let the backend deduce param type
//			param,	// char * const *paramValues,
//			NULL,	// int *paramLengths, don't need param lengths since param type is text
//			NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//			1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//	if(PQresultStatus(op) == PGRES_COMMAND_OK)
//		zlog_debug(worker->dispatcher->log, "Delete %s from DB dispatcher.workers success", worker->hostName);
//	else
//		zlog_error(worker->dispatcher->log, "Delete %s from DB dispatcher.workers failed: %s", worker->hostName, PQerrorMessage(worker->dispatcher->dbconn));
 #if defined TASKPROC_IN_MULTITHREAD
	if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
	if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
			"DELETE FROM dispatcher.workers WHERE name='%s'", worker->hostName))
		zlog_debug(worker->dispatcher->log, "Delete %s from DB dispatcher.workers success", worker->hostName);
	else
		zlog_error(worker->dispatcher->log, "Delete %s from DB dispatcher.workers failed: %s", worker->hostName, PQerrorMessage(worker->dispatcher->dbconn));
	// [DB] ========================================
#endif

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
#ifdef SAVE_STATE_TO_DATABASE
				// FIXME: [DB] add task to database
				// [DB] ========================================
 #if defined TASKPROC_IN_MULTITHREAD
				if(DBExecCmd(self->dbconn, &self->db_write_lock,
 #else
				if(DBExecCmd(self->dbconn, NULL,
 #endif
						"INSERT INTO dispatcher.tasks (taskID, status, dispatched, \
						create_time, timeout_old, timeout_new, timeout_interval, \
						service_name, method, data, client_str, worker_str) \
						VALUES ('%s', %d, %d, %lu, %lu, %lu, %lu, '%s', '%s', '%s', '%s', '%s')",
						task_get_taskID(task), task_get_status(task), task_get_dispatched(task),
						task_get_createTime(task), timeout_get_old(task_get_timeout(task)), timeout_get_new(task_get_timeout(task)), timeout_get_interval(task_get_timeout(task)),
						task_get_serviceName(task), task_get_method(task),task_get_data(task), task_get_client_str(task), task_get_worker_str(task)
						))
					zlog_debug(self->log, "Insert task ID [%s] to DB dispatcher.tasks success", task_get_taskID(task));
				else
					zlog_error(self->log, "Insert task ID [%s] to DB dispatcher.tasks failed: %s", task_get_taskID(task), PQerrorMessage(self->dbconn));
				// [DB] ========================================
#endif

				zlog_debug(self->log, "Create task:");
				zlog_debug(self->log, "  taskID = %s", task_get_taskID(task) ? task_get_taskID(task) : "NULL");
				zlog_debug(self->log, "  serviceName = %s", task_get_serviceName(task) ? task_get_serviceName(task) : "NULL");
				zlog_debug(self->log, "  client = %s", task_get_client_str(task) ? task_get_client_str(task) : "NULL");
				zlog_debug(self->log, "  method = %s", task_get_method(task) ? task_get_method(task) : "NULL");
				zlog_debug(self->log, "  data = %s", task_get_data(task) ? task_get_data(task) : "NULL");
				zlog_debug(self->log, "  worker = %s", task_get_worker_str(task) ? task_get_worker_str(task) : "NULL");

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
    			_sendcmd(self, sender, 3, cmd_code2payload(TASKQUERYREP), taskid, status);
    		}
    		else {
    			task_t *task = zhash_lookup(self->tasks, taskid);
				// Task found, reply percentage (could be percentage, DISPATCHING or FAIL
    			if(task) {
    				zlog_info(self->log, "Receive TASKQUERYREQ with task ID [%s] and found, return %3d%% to client", taskid, task_get_status(task));
    				status = uitoa(task_get_status(task));
    				_sendcmd(self, sender, 3, cmd_code2payload(TASKQUERYREP), taskid, status);
    				free(status);

    				// Q: After query, if task status percentage is 100/FAIL, remove it from self->tasks?
					// A: No for now, because client may query several times.
    			}
    			// Task not found, reply S_NOTFOUND
    			else {
    				zlog_info(self->log, "Receive TASKQUERYREQ with task ID [%s] but not found, return FAIL to client", taskid);
    				status = stat_code2payload(S_NOTFOUND);
    				_sendcmd(self, sender, 3, cmd_code2payload(TASKQUERYREP), taskid, status);
    			}
    		}

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
				char *service_name = zmsg_popstr(msg); // fetch service name
				if(service_name) {
					worker->service = service_require(self, service_name);
					free(service_name);
				}
				worker->address = zframe_dup(sender);
				worker->heartbeat = heartbeat_create(HEARTBEAT_LIVEING, HEARTBEAT_INTERVAL, _heartbeat_sendfn, (void *)worker);
				zhash_insert(self->workers, hostName, worker);
				zhash_freefn(self->workers, hostName, worker_destroy);
				zlist_append(worker->service->workers, worker);
#ifdef SAVE_STATE_TO_DATABASE
				// FIXME: [DB] add worker to database
				// [DB] ========================================
 #if defined TASKPROC_IN_MULTITHREAD
				if(DBExecCmd(self->dbconn, &self->db_write_lock,
 #else
				if(DBExecCmd(self->dbconn, NULL,
 #endif
						"INSERT INTO dispatcher.workers \
						(name, service_name, \
						heartbeat_deadtime, heartbeat_keepalive, \
						timeout_old, timeout_new, timeout_interval, retries) \
						VALUES ('%s', '%s', %lu, %lu, %lu, %lu, %lu, %d)",
						worker->hostName, worker->service->name,
						heartbeat_get_deadtime(worker->heartbeat), heartbeat_get_keepalive(worker->heartbeat),
						timeout_get_old(heartbeat_get_timeout(worker->heartbeat)),
						timeout_get_new(heartbeat_get_timeout(worker->heartbeat)),
						timeout_get_interval(heartbeat_get_timeout(worker->heartbeat)),
						heartbeat_get_retries(worker->heartbeat)
						))
					zlog_debug(self->log, "Insert worker [%s] to DB dispatcher.workers success", worker->hostName);
				else
					zlog_error(self->log, "Insert worker [%s] to DB dispatcher.workers failed: %s", worker->hostName, PQerrorMessage(self->dbconn));
				// [DB] ========================================
#endif

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
			// Task found, update percentage & modifyTime
			if(task) {
				// receive S_OK, update task status
				if(stat_payload2code(code) == S_OK) {
					zlog_info(self->log, "Receive TASKDISREP [S_OK] for task ID [%s] from worker [%s], update status to 0", taskid, hostName);
					task_set_status(task, 0);
#ifdef SAVE_STATE_TO_DATABASE
					// FIXME: [DB] update task status to 0 as initial in database
					// [DB] ========================================
//					const char *params[1] = { taskid };
//					PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//							"UPDATE dispatcher.tasks SET status='0' WHERE taskID=$1",
//							1,		// int nParams
//							NULL,	// Oid *paramTypes, let the backend deduce param type
//							params,	// char * const *paramValues,
//							NULL,	// int *paramLengths, don't need param lengths since param type is text
//							NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//							1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//					if(PQresultStatus(op) == PGRES_COMMAND_OK)
//						zlog_debug(worker->dispatcher->log, "Update task [%s] status to [0%%] to DB dispatcher.tasks success", taskid);
//					else
//						zlog_error(worker->dispatcher->log, "Update task [%s] status to [0%%] to DB dispatcher.tasks failed: %s", taskid, PQerrorMessage(worker->dispatcher->dbconn));
 #if defined TASKPROC_IN_MULTITHREAD
					if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
					if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
							"UPDATE dispatcher.tasks SET status='0' WHERE taskID='%s'", taskid))
						zlog_debug(worker->dispatcher->log, "Update task [%s] status to [0%%] to DB dispatcher.tasks success", taskid);
					else
						zlog_error(worker->dispatcher->log, "Update task [%s] status to [0%%] to DB dispatcher.tasks failed: %s", taskid, PQerrorMessage(worker->dispatcher->dbconn));
					// [DB] ========================================
#endif
				}
				// receive S_FAIL, set task status to FAIL
				else {
					zlog_info(self->log, "Receive TASKDISREP [S_FAIL] for task ID [%s] from worker [%s], update status to FAIL", taskid, hostName);
					task_set_status(task, FAIL);
#ifdef SAVE_STATE_TO_DATABASE
					// FIXME: [DB] update task status to FAIL in database
					// [DB] ========================================
//					char *per_str = uitoa(FAIL);
//					const char *params[2] = { per_str, taskid };
//					PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//							"UPDATE dispatcher.tasks SET status=$1 WHERE taskID=$2",
//							2,		// int nParams
//							NULL,	// Oid *paramTypes, let the backend deduce param type
//							params,	// char * const *paramValues,
//							NULL,	// int *paramLengths, don't need param lengths since param type is text
//							NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//							1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//					if(PQresultStatus(op) == PGRES_COMMAND_OK)
//						zlog_debug(worker->dispatcher->log, "Update task [%s] status to FAIL(%s) to DB dispatcher.tasks success", taskid, per_str);
//					else
//						zlog_error(worker->dispatcher->log, "Update task [%s] status to FAIL(%s) to DB dispatcher.tasks failed: %s", taskid, per_str, PQerrorMessage(worker->dispatcher->dbconn));
//					FREE(per_str);
 #if defined TASKPROC_IN_MULTITHREAD
					if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
					if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
							"UPDATE dispatcher.tasks SET status=%d WHERE taskID='%s'", FAIL, taskid))
						zlog_debug(worker->dispatcher->log, "Update task [%s] status to FAIL(%d) to DB dispatcher.tasks success", taskid, FAIL);
					else
						zlog_error(worker->dispatcher->log, "Update task [%s] status to FAIL(%d) to DB dispatcher.tasks failed: %s", taskid, FAIL, PQerrorMessage(worker->dispatcher->dbconn));
					// [DB] ========================================
#endif
				}
			}
			// Task not-found, discard it silently
			else {
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
#ifdef SAVE_STATE_TO_DATABASE
				// FIXME: [DB] update task status in database
				// [DB] ========================================
//				char *per_str = uitoa(per);
//				const char *params[2] = { per_str, taskid };
//				PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//						"UPDATE dispatcher.tasks SET status=$1 WHERE taskID=$2",
//						2,		// int nParams
//						NULL,	// Oid *paramTypes, let the backend deduce param type
//						params,	// char * const *paramValues,
//						NULL,	// int *paramLengths, don't need param lengths since param type is text
//						NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//						1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//				if(PQresultStatus(op) == PGRES_COMMAND_OK)
//					zlog_debug(worker->dispatcher->log, "Update task [%s] status to [%3u%%] to DB dispatcher.tasks success", taskid, per);
//				else
//					zlog_error(worker->dispatcher->log, "Update task [%s] status to [%3u%%] to DB dispatcher.tasks failed: %s", taskid, per, PQerrorMessage(worker->dispatcher->dbconn));
//				FREE(per_str);
 #if defined TASKPROC_IN_MULTITHREAD
				if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
				if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
						"UPDATE dispatcher.tasks SET status=%d WHERE taskID='%s'", per, taskid))
					zlog_debug(worker->dispatcher->log, "Update task [%s] status to [%3u%%] to DB dispatcher.tasks success", taskid, per);
				else
					zlog_error(worker->dispatcher->log, "Update task [%s] status to [%3u%%] to DB dispatcher.tasks failed: %s", taskid, per, PQerrorMessage(worker->dispatcher->dbconn));
				// [DB] ========================================
#endif
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
    	if(worker_exists) {
    		heartbeat_reset_retries(worker->heartbeat);
#ifdef SAVE_STATE_TO_DATABASE
    		// FIXME: [DB] update worker->heartbeat->retries in database
    		// [DB] ========================================
//			char *retries_str = uitoa(heartbeat_get_retries(worker->heartbeat));
//			const char *params[2] = { retries_str, worker->hostName };
//			PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//					"UPDATE dispatcher.workers SET retries=$1 WHERE name=$2",
//					2,		// int nParams
//					NULL,	// Oid *paramTypes, let the backend deduce param type
//					params,	// char * const *paramValues,
//					NULL,	// int *paramLengths, don't need param lengths since param type is text
//					NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//					1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//			if(PQresultStatus(op) == PGRES_COMMAND_OK)
//				zlog_debug(worker->dispatcher->log, "Update worker [%s]'s retries to %s to DB dispatcher.workers success", worker->hostName, retries_str);
//			else
//				zlog_error(worker->dispatcher->log, "Update worker [%s]'s retries to %s to DB dispatcher.workers failed: %s", worker->hostName, retries_str, PQerrorMessage(worker->dispatcher->dbconn));
//			FREE(retries_str);
 #if defined TASKPROC_IN_MULTITHREAD
			if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
			if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
					"UPDATE dispatcher.workers SET retries=%d WHERE name='%s'", heartbeat_get_retries(worker->heartbeat), worker->hostName))
				zlog_debug(worker->dispatcher->log, "Update worker [%s]'s retries to %d to DB dispatcher.workers success", worker->hostName, heartbeat_get_retries(worker->heartbeat));
			else
				zlog_error(worker->dispatcher->log, "Update worker [%s]'s retries to %d to DB dispatcher.workers failed: %s", worker->hostName, heartbeat_get_retries(worker->heartbeat), PQerrorMessage(worker->dispatcher->dbconn));
			// [DB] ========================================
#endif
    	}
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

unsigned int _dispatching_count = 0;
pthread_mutex_t _dispatching_count_lock;

void
_dispatching_count_set(unsigned int count)
{
	if(pthread_mutex_lock(&_dispatching_count_lock) == 0) {
		_dispatching_count = count;
		pthread_mutex_unlock(&_dispatching_count_lock);
	}
}

unsigned int
_dispatching_count_get()
{
	unsigned int ret = -1;
	if(pthread_mutex_lock(&_dispatching_count_lock) == 0) {
		ret = _dispatching_count;
		pthread_mutex_unlock(&_dispatching_count_lock);
	}
	return ret;
}

void
_dispatching_count_decraese()
{
	if(pthread_mutex_lock(&_dispatching_count_lock) == 0) {
		_dispatching_count--;
		pthread_mutex_unlock(&_dispatching_count_lock);
	}
}

static void
_service_dispatch(void *ptr)
{
	if(!ptr) goto _service_dispatch_return;

	service_t *self = (service_t *)ptr;

	// Remove dead workers and do heartbeat before checking task
	// size. We need to send heartbeat even there is no task.
	for(worker_t *worker = (worker_t *)zlist_first(self->workers);
			worker != NULL;
			worker = (worker_t *)zlist_next(self->workers)) {
#ifdef SAVE_STATE_TO_DATABASE
		// FIXME: [DB] if heartbeat retries is different from previous, update DB
		int retries_old = heartbeat_get_retries(worker->heartbeat);
#endif
		if(heartbeat_check(worker->heartbeat) == 0) {
			zlog_info(self->dispatcher->log, "Worker [%s] was dead, delete it", worker->hostName);
			worker_delete(worker, 1);
			// Due to worker delete, no need to update heartbeat retries in DB
		}
#ifdef SAVE_STATE_TO_DATABASE
		// FIXME: [DB] if heartbeat retries is different from previous, update DB
		// [DB] ========================================
		else {
			int retries_new = heartbeat_get_retries(worker->heartbeat);
			if(retries_new != retries_old) {
//				char *retries_str = uitoa(retries_new);
//				const char *params[2] = { retries_str, worker->hostName };
//				PGresult *op = PQexecParams(worker->dispatcher->dbconn,
//						"UPDATE dispatcher.workers SET retries=$1 WHERE name=$2",
//						2,		// int nParams
//						NULL,	// Oid *paramTypes, let the backend deduce param type
//						params,	// char * const *paramValues,
//						NULL,	// int *paramLengths, don't need param lengths since param type is text
//						NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//						1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//				if(PQresultStatus(op) == PGRES_COMMAND_OK)
//					zlog_debug(worker->dispatcher->log, "Update worker [%s]'s retries to %s to DB dispatcher.workers success", worker->hostName, retries_str);
//				else
//					zlog_error(worker->dispatcher->log, "Update worker [%s]'s retries to %s to DB dispatcher.workers failed: %s", worker->hostName, retries_str, PQerrorMessage(worker->dispatcher->dbconn));
//				FREE(retries_str);
 #if defined TASKPROC_IN_MULTITHREAD
				if(DBExecCmd(worker->dispatcher->dbconn, &worker->dispatcher->db_write_lock,
 #else
				if(DBExecCmd(worker->dispatcher->dbconn, NULL,
 #endif
						"UPDATE dispatcher.workers SET retries=%d WHERE name='%s'",	retries_new, worker->hostName))
					zlog_debug(worker->dispatcher->log, "Update worker [%s]'s retries to %d to DB dispatcher.workers success", worker->hostName, retries_new);
				else
					zlog_error(worker->dispatcher->log, "Update worker [%s]'s retries to %d to DB dispatcher.workers failed: %s", worker->hostName, retries_new, PQerrorMessage(worker->dispatcher->dbconn));

			}
		}
		// [DB] ========================================
#endif
	}

    size_t count = zlist_size(self->tasks);
    if(count > 0)	zlog_debug(self->dispatcher->log, "[%s] service dispatching:", self->name);
    else			goto _service_dispatch_return;

    while(count-- > 0) {
        task_t *task = (task_t *)zlist_pop(self->tasks);

        // Task was done (i.e. dispatched is true and status is 100/FAIL), we don't need
        // to take care of it anymore, remove it by not pushing back to task list
        if(task_get_dispatched(task) && ((task_get_status(task) == 100) || (task_get_status(task) == FAIL)))
        	continue;

        // If task is timeout'd, remove it by not pushing back to task list and change status to FAIL
        //
        // FIXME: [DB] update task timeout as in task_get_expired() <--- don't need to update anything
        if(task_get_expired(task)) {
        	zlog_info(self->dispatcher->log, "Task [%s] was timeout'd, change status to FAIL and won't be dispatch anymore", task_get_taskID(task));
        	task_set_status(task, FAIL);
#ifdef SAVE_STATE_TO_DATABASE
        	// FIXME: [DB] update task status to FAIL in database
        	// [DB] ========================================
//			char *per_str = uitoa(FAIL);
//			const char *params[2] = { per_str, task_get_taskID(task) };
//			PGresult *op = PQexecParams(self->dispatcher->dbconn,
//					"UPDATE dispatcher.tasks SET status=$1 WHERE taskID=$2",
//					2,		// int nParams
//					NULL,	// Oid *paramTypes, let the backend deduce param type
//					params,	// char * const *paramValues,
//					NULL,	// int *paramLengths, don't need param lengths since param type is text
//					NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//					1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//			if(PQresultStatus(op) == PGRES_COMMAND_OK)
//				zlog_debug(self->dispatcher->log, "Update task [%s] status to FAIL(%s) to DB dispatcher.tasks success", task_get_taskID(task), per_str);
//			else
//				zlog_error(self->dispatcher->log, "Update task [%s] status to FAIL(%s) to DB dispatcher.tasks failed: %s", task_get_taskID(task), per_str, PQerrorMessage(self->dispatcher->dbconn));
//			FREE(per_str);
#if defined TASKPROC_IN_MULTITHREAD
        	if(DBExecCmd(self->dispatcher->dbconn, &self->dispatcher->db_write_lock,
#else
        	if(DBExecCmd(self->dispatcher->dbconn, NULL,
#endif
        			"UPDATE dispatcher.tasks SET status=%d WHERE taskID='%s'", FAIL, task_get_taskID(task)))
				zlog_debug(self->dispatcher->log, "Update task [%s] status to FAIL(%d) to DB dispatcher.tasks success", task_get_taskID(task), FAIL);
			else
				zlog_error(self->dispatcher->log, "Update task [%s] status to FAIL(%d) to DB dispatcher.tasks failed: %s", task_get_taskID(task), FAIL, PQerrorMessage(self->dispatcher->dbconn));
			// [DB] ========================================
#endif
			continue;
        }

        // If task wasn't done and not timeout'd, check if the task
        // is dispatched or not. If not, try to dispatch it
		if(!task_get_dispatched(task)) {
			worker_t *worker = NULL;
			// To specific worker
			if(task_get_worker(task)) {
				worker = zhash_lookup(self->dispatcher->workers, task_get_worker_str(task));
				// Can't find specific worker, push back to the end of task list
				if(!worker) {
					zlog_debug(self->dispatcher->log, "Task [%s] specify worker [%s], but can't found", task_get_taskID(task), task_get_worker_str(task));
				}
				// Find specific worker, go on
				else {
					zlist_remove(self->workers, worker); // remove worker first, will append back after dispatching
				}
			}
			// To service
			else {
				// pop up the first worker to process task
				if(zlist_size(self->workers) > 0) {
					worker = (worker_t*)zlist_pop(self->workers);
					task_set_worker(task, worker->address);
				}
			}

			if(worker) {
				zlog_info(self->dispatcher->log, "Dispatching task [%s] to worker [%s]", task_get_taskID(task), worker->hostName);
				_sendcmd(self->dispatcher, worker->address,
						4, cmd_code2payload(TASKDISREQ), task_get_taskID(task), task_get_method(task), task_get_data(task));
				task_set_dispatched(task, 1);
#ifdef SAVE_STATE_TO_DATABASE
				// FIXME: [DB] update task worker_str and dispatched to 1 in database
				// [DB] ========================================
//				const char *params[2] = { task_get_worker_str(task), task_get_taskID(task) };
//				PGresult *op = PQexecParams(self->dispatcher->dbconn,
//						"UPDATE dispatcher.tasks SET dispatched='1', worker_str=$1 WHERE taskID=$2",
//						2,		// int nParams
//						NULL,	// Oid *paramTypes, let the backend deduce param type
//						params,	// char * const *paramValues,
//						NULL,	// int *paramLengths, don't need param lengths since param type is text
//						NULL,	// int *paramFormats, 0 - text, 1 - binary, default to all text params
//						1);		// int resultFormat, 0 - results in text foramt, 1 - binary
//				if(PQresultStatus(op) == PGRES_COMMAND_OK)
//					zlog_debug(self->dispatcher->log, "Update task [%s] dispatched to 1 to DB dispatcher.tasks success", task_get_taskID(task));
//				else
//					zlog_error(self->dispatcher->log, "Update task [%s] dispatched to 1 to DB dispatcher.tasks failed: %s", task_get_taskID(task), PQerrorMessage(self->dispatcher->dbconn));
 #if defined TASKPROC_IN_MULTITHREAD
				if(DBExecCmd(self->dispatcher->dbconn, &self->dispatcher->db_write_lock,
 #else
				if(DBExecCmd(self->dispatcher->dbconn, NULL,
 #endif
						"UPDATE dispatcher.tasks SET dispatched='1', worker_str='%s' WHERE taskID='%s'", task_get_worker_str(task), task_get_taskID(task)))
					zlog_debug(self->dispatcher->log, "Update task [%s] dispatched to 1 to DB dispatcher.tasks success", task_get_taskID(task));
				else
					zlog_error(self->dispatcher->log, "Update task [%s] dispatched to 1 to DB dispatcher.tasks failed: %s", task_get_taskID(task), PQerrorMessage(self->dispatcher->dbconn));
				// [DB] ========================================
#endif

				// Workers are scheduled in the round-robin fashion
				zlist_append(self->workers, worker); // push worker back to on-duty list
			}
		}
        zlist_append(self->tasks, task); // push task back to the end of task list
    }

_service_dispatch_return:
	_dispatching_count_decraese();
	return;
}

int
_service_dispatch_all(const char *key, void *item, void *argument)
{
	service_t *service = item;
	if(service) {
#ifdef TASKPROC_IN_MULTITHREAD
		if(service->dispatcher->threads) {
			if(threadpool_add(service->dispatcher->threads, &_service_dispatch, service, 0) == 0) {
				; //zlog_debug(service->dispatcher->log, "Dispatch [%s] service by threadpool", service->name);
			}
			else {
				zlog_debug(service->dispatcher->log, "Dispatch [%s] service by current thread due to no thread available from threadpool", service->name);
				_service_dispatch(service);
			}
		}
		else
#endif
			_service_dispatch(service);
	}
	return 0;
}

void service_dispatch_all(dispatcher_t *self)
{
	if(_dispatching_count_get() == 0) {
		_dispatching_count_set(zhash_size(self->services));
		zhash_foreach(self->services, &_service_dispatch_all, NULL); // heartbeat is also manipulated in service_distpach
	}
}

int disconnect_all_workers(const char *key, void *item, void *argument)
{
	if(item) worker_delete((worker_t *)item, 1);
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

	dispatcher_t *self = dispatcher_new();
	if(!self) return 1;
	zsocket_bind(self->socket, argv[1]);
	zlog_info(self->log, "Bind to %s", argv[1]);

	int quit = 0;
	// Get and process messages forever or until interrupted
	while(!quit) {
		zmq_pollitem_t items [] = {	{ self->socket,  0, ZMQ_POLLIN, 0 } };
#ifdef TASKPROC_IN_MULTITHREAD
		if(self->threads) {
			if(pthread_mutex_lock(&(self->sock_lock)) != 0) {
				zlog_error(self->log, "Unable to lock mutex for zmsg_poll()");
				continue;
			}
		}
#endif
		int rc = zmq_poll (items, 1, 100 * ZMQ_POLL_MSEC);
#ifdef TASKPROC_IN_MULTITHREAD
		if(self->threads) {
			if(pthread_mutex_unlock(&(self->sock_lock)) != 0) {
				zlog_error(self->log,"Failed to unlock mutex for zmsg_poll(), terminated...");
				quit = 1;
			}
		}
#endif
		if (rc == -1) break; //  Interrupted -> zctx_interrupted

		//  Process next input message, if any
		if (items [0].revents & ZMQ_POLLIN) {
#ifdef TASKPROC_IN_MULTITHREAD
			if(self->threads) {
				if(pthread_mutex_lock(&(self->sock_lock)) != 0) {
					zlog_error(self->log, "Unable to lock mutex for zmsg_recv(), do not receive message");
					continue;
				}
			}
#endif
			zmsg_t *msg = zmsg_recv(self->socket);
#ifdef TASKPROC_IN_MULTITHREAD
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

		service_dispatch_all(self);
	}
	if (zctx_interrupted) {
		zlog_info(self->log, "Interrupt received, shutting down...");
		zhash_foreach(self->workers, &disconnect_all_workers, NULL); // Send DISCONNECT to all connected workers

		// TODO: backup database to persistence storage
	}

	dispatcher_destroy(&self);
	return 0;
}

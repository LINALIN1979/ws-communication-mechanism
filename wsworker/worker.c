#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option
#include "worker.h"
#include <pthread.h>


#define RECV_WAIT_TIMEOUT			3000 // default waiting timeout for receive message

#define WORKER_STATE_UNREGISTER		1
#define WORKER_STATE_REGISTERING	2
#define WORKER_STATE_REGISTERED		3

// Structure of worker class
// Access these properties only via class methods
struct _worker_t {
	zctx_t*		ctx;				// Our context
	char*		dispatcher;			// Dispatcher address, ex: "tcp://172.17.153.190:5555"
	void*		socket;				// Socket to dispatcher
//	uint64_t	recvWaitTimeout;	// Waiting timeout (millisecond) for message receiving

	char*		serviceName;		// Worker service name
	char*		hostName;			// Worker host name

	int			workerState;		// Worker state for state machine control

	char*		ipcAddress;			// IPC address, "inproc://" + hostName
	void*		ipcBindSocket;		// IPC socket binds to

	pthread_t	workerThread;		// Thread for state machine control

	worker_oplong_fn*	oplong;		// callback when receive oplong call
	worker_opshort_fn*	opshort;	// callback when receive opshort call

	heartbeat_t*		heartbeat;	// Heartbeat

	zlog_category_t*	log;

	pthread_mutex_t		sock_lock;
};

// Connect/reconnect to dispatcher
static void
_worker_connect_to_dispatcher(worker_t *self)
{
	if(self) {
		if(self->socket)
			zsocket_destroy(self->ctx, self->socket);
		if(!(self->socket = zsocket_new(self->ctx, ZMQ_DEALER))) {
			zlog_error(self->log, "zsocket_new() return NULL, create socket failed");
			return;
		}

		// ----
		// Add "-XXXXXXXX" (XXXXXXXX is a random string) after hostName
		// when setup socket identity.
		// This is the workaround solution of the issue when reconnect to
		// ROUTER with same identity, ROUTER will refuse the connection.
		int hostName_len = strlen(self->hostName);
		char *identity = (char*)zmalloc(sizeof(char) * (hostName_len + 10));
		if(identity) {
			memcpy(identity, self->hostName, hostName_len);
			identity[hostName_len] = '-';
			rand_str(identity + hostName_len + 1, 8);
			zmq_setsockopt(self->socket, ZMQ_IDENTITY, identity, strlen(identity));
			free(identity);
		}
		else
			zmq_setsockopt(self->socket, ZMQ_IDENTITY, self->hostName, hostName_len + 9);
		// ----

		if(zmq_connect(self->socket, self->dispatcher) == 0) {
			zlog_info(self->log, "Connect to dispatcher at %s...success", self->dispatcher);
			zlog_info(self->log, "Send SERVICEREGREQ to dispatcher");
			sendcmd(&self->sock_lock, self->log, self->socket, WORKER, SERVICEREGREQ, 1, self->serviceName);
			self->workerState = WORKER_STATE_REGISTERING;
			heartbeat_reactivate(self->heartbeat); // count from try to connect
		}
		else {
			zlog_error(self->log, "Connect to dispatcher at %s...failed, error code = %d", self->dispatcher, errno);
		}
	}
}

#define THREADCOMM_TERMINATE		"TERMINATE"

static void *
_worker_state_machine(void *ptr)
{
	worker_t* self = (worker_t *)ptr;
	if(!self) return NULL;

	void *ipc = zsocket_new(self->ctx, ZMQ_PAIR);
	if(zmq_connect(ipc, self->ipcAddress))
		zlog_error(self->log, "zmq_connect() failed to connect parent process, error code = %d", errno);
	else
		zlog_info(self->log, "Worker thread connects to parent process at %s", self->ipcAddress);

	_worker_connect_to_dispatcher(self);

	zmsg_t		*msg = NULL;
	zframe_t	*first_frame, *empty_frame, *from_frame;
	int			rc, ret_code;
	char 		*cmd, *code, *taskid, *method, *data, *token, *client, *ret_array;
	int quit = 0;
	while(!quit) {
		zmq_pollitem_t	items[] = {
				{self->socket,	0, ZMQ_POLLIN, 0},
				{ipc,			0, ZMQ_POLLIN, 0},
		};
		if(pthread_mutex_lock(&self->sock_lock) != 0) {
			zlog_error(self->log, "Unable to lock mutex for zmsg_poll()");
			continue;
		}
		rc = zmq_poll(items, 2, 100 * ZMQ_POLL_MSEC);
		if(pthread_mutex_unlock(&self->sock_lock) != 0) {
			zlog_error(self->log,"Failed to unlock mutex for zmsg_poll(), terminated...");
			quit = 1;
		}
		if(rc == -1) break; // Interrupted

		// ====================
		// Handle message from other thread
		if(items[1].revents & ZMQ_POLLIN) {
			zlog_info(self->log, "Receiving from other thread...");
			msg = zmsg_recv(ipc);
			if(!msg) break; // Interrupted

			first_frame = zmsg_pop(msg);
			if(zframe_streq(first_frame, THREADCOMM_TERMINATE)) {
				zlog_info(self->log, "  Receive terminate command");
				quit = 1;
			}
			zframe_destroy(&first_frame);
			zmsg_destroy(&msg);
		}
		// ====================

		// ====================
		// Handle message from dispatcher
		if(items[0].revents & ZMQ_POLLIN) {
			zlog_debug(self->log, "Receiving from dispatcher...");
			if(pthread_mutex_lock(&self->sock_lock) != 0) {
				zlog_error(self->log, "Unable to lock mutex for zmsg_recv(), do not receive message");
				continue;
			}
			msg = zmsg_recv(self->socket);
			if(pthread_mutex_unlock(&self->sock_lock) != 0) {
				zlog_error(self->log,"Failed to unlock mutex for zmsg_recv(), terminated...");
				quit = 1;
			}
			if(!msg) break; // Interrupted

			dumpzmsg(self->log, msg);

			// Message length is not enough, bypass it
			if(!is_zmsg_size_enough(self->log, msg, 2)) {
				zmsg_destroy(&msg);
				continue;
			}

			// Empty frame:
			//   DEALER socket has to remove empty frame manually after receiving
			empty_frame = zmsg_pop(msg);
			assert(zframe_streq(empty_frame, ""));
			zframe_destroy(&empty_frame);
			// From frame:
			from_frame = zmsg_pop(msg);
			if(!zframe_streq(from_frame, DISPATCHER)) {
				// Message is not from dispatcher, ignore it
				zlog_debug(self->log, "Message is not from DISPATCHER, ignore it");
				zframe_destroy(&from_frame);
				zmsg_destroy(&msg);
				continue;
			}
			zframe_destroy(&from_frame);

			switch(self->workerState)
			{
//			case WORKER_STATE_UNREGISTER:
//				zclock_log("I: try to reconnect to dispatcher");
//				_worker_connect_to_dispatcher(self);
//				break;

			case WORKER_STATE_REGISTERING:
				if(is_zmsg_size_enough(self->log, msg, 2)) {
					cmd = zmsg_popstr(msg);
					code = zmsg_popstr(msg);

					switch(cmd_payload2code(cmd))
					{
					case SERVICEREGREP:
						if(stat_payload2code(code) == S_OK) {
							zlog_info(self->log, "SERVICEREG success");
							self->workerState = WORKER_STATE_REGISTERED;
							heartbeat_reactivate(self->heartbeat); // enable again for WORKER_STATE_REGISTERED loop
						}
						else {
							zlog_info(self->log, "SERVICEREG failed");
							self->workerState = WORKER_STATE_UNREGISTER;
						}
						break;

					case DISCONNECT:
						zlog_info(self->log, "Disconnected by dispatcher");
						self->workerState = WORKER_STATE_UNREGISTER;
						break;

					// Ignore unrelated messages
					default:
						zlog_debug(self->log, "In WORKER_STATE_REGISTERING do not accept command [%s]", cmd_code2str(cmd_payload2code(cmd)));
						break;
					} // switch(cmd_payload2code(cmd))

					FREE(cmd);
					FREE(code);
				}
				break;

			case WORKER_STATE_REGISTERED:
				if(is_zmsg_size_enough(self->log, msg, 1)) {
					cmd = zmsg_popstr(msg);

					switch(cmd_payload2code(cmd))
					{
					// oplong
					case TASKDISREQ:
						if(is_zmsg_size_enough(self->log, msg, 3)) {
							taskid = zmsg_popstr(msg);
							method = zmsg_popstr(msg);
							data = zmsg_popstr(msg);

							zlog_info(self->log, "Receive OPLONG task from dispatcher");
							if(taskid) {
								zlog_debug(self->log, "  ID: %s", taskid ? taskid : "empty string");
								zlog_debug(self->log, "  Method: %s", method ? method : "empty string");
								zlog_debug(self->log, "  Data: %s", data ? data : "empty string");
								ret_code = S_FAIL;
								if(self->oplong) {
									if(self->oplong(taskid, method, data) == OPLONG_ACCEPT)
										ret_code = S_OK;
								}
								zlog_info(self->log, "Reply [%s] to task ID [%s]", stat_code2str(ret_code), taskid);
								sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKDISREP, 2, taskid, stat_code2payload(ret_code));
							}
							else zlog_info(self->log, "  WTF, no task ID");

							FREE(taskid);
							FREE(method);
							FREE(data);
						}
						break;

					// opshort
					case TASKDIRECTREQ:
						if(is_zmsg_size_enough(self->log, msg, 4)) {
							token = zmsg_popstr(msg);
							client = zmsg_popstr(msg);
							method = zmsg_popstr(msg);
							data = zmsg_popstr(msg);

							zlog_info(self->log, "Receive OPSHORT task from dispatcher");
							if(token) {
								zlog_debug(self->log, "  Token: %s", token ? token : "empty string");
								zlog_debug(self->log, "  Client: %s", client ? client : "empty string");
								zlog_debug(self->log, "  Method: %s", method ? method : "empty string");
								zlog_debug(self->log, "  Data: %s", data ? data : "empty string");
								if(self->opshort) {
									ret_array = self->opshort(method, data);
									if(ret_array) {
										ret_array = strdup(ret_array);
										if(ret_array) {
											zlog_info(self->log, "Reply [%s] to client [%s]'s task which token ID is [%s]", ret_array, client, token);
											sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKDIRECTREP, 3, token, client, ret_array);
											free(ret_array);
										}
										else {
											// Supposedly not to reach here, but we still do error handling
											zlog_info(self->log, "Failed to copy string from opshort, reply empty data to client [%s]", client);
											sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKDIRECTREP, 3, token, client, "");
										}
									}
									else {
										// Returned string from self->opshort is null, reply empty string
										zlog_info(self->log, "Reply empty data to client [%s]", client);
										sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKDIRECTREP, 3, token, client, "");
									}
								}
								else {
									// No opshort registered, reply empty string
									zlog_info(self->log, "No OPSHORT callback registered, reply empty data");
									sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKDIRECTREP, 3, token, client, "");
								}
							}
							else zlog_info(self->log, "  WTF, no token");

							FREE(token);
							FREE(client);
							FREE(method);
							FREE(data);
						}
						break;

					// Receive heartbeat, update heartbeat retries
					case HEARTBEAT:
						heartbeat_reset_retries(self->heartbeat);
						break;

					case DISCONNECT:
						zlog_info(self->log, "Disconnected by dispatcher");
						self->workerState = WORKER_STATE_UNREGISTER;
						break;

					default: break;
					} // switch(cmd_payload2code(cmd))

					FREE(cmd);
				}
				break;

			default: break;
			} // switch(self->workerState)
			zmsg_destroy(&msg);
		}
		// ====================

		// ====================
		// Heartbeat control to dispatcher
		if(self->workerState == WORKER_STATE_REGISTERED) {
			if(heartbeat_check(self->heartbeat) == 0) {
				zlog_info(self->log, "Can't feel dispatcher's heartbeat!");
				sendcmd(&self->sock_lock, self->log, self->socket, WORKER, DISCONNECT, 0);
				self->workerState = WORKER_STATE_UNREGISTER;
				heartbeat_reactivate(self->heartbeat); // enable again for WORKER_STATE_REGISTERING loop
			}
		}
		else if(self->workerState == WORKER_STATE_REGISTERING) {
			if(heartbeat_check(self->heartbeat) == 0) {
				zlog_info(self->log, "Previous trial to connect dispatcher was timeout'd");
				self->workerState = WORKER_STATE_UNREGISTER;
			}
		}
		// Retry connect if state becomes WORKER_STATE_UNREGISTER
		if(self->workerState == WORKER_STATE_UNREGISTER) {
			zlog_info(self->log, "Try to reconnect to dispatcher");
			_worker_connect_to_dispatcher(self);
		}
		// ====================
	}
	if (zctx_interrupted) {
		// Worker was interrupted
		zlog_info(self->log, "Interrupt received, killing worker...");
	}
	self->workerState = WORKER_STATE_UNREGISTER;
	sendcmd(&self->sock_lock, self->log, self->socket, WORKER, DISCONNECT, 0);
//	zmq_close(ipc); // <--- DON'T DO SO, it occurs segmentation fault. zctx_destroy in worker_destroy will cover
	pthread_exit(NULL); // int ret; pthread_exit(&ret);
	return NULL;
}

void
_heartbeat_sendfn(void *ptr)
{
	worker_t *self = (worker_t *)ptr;
	if(self) {
		// Only send HEARTBEAT when connected
		if(self->workerState == WORKER_STATE_REGISTERED) {
			//zlog_info(self->log, "Sending HEARTBEAT");
			sendcmd(&self->sock_lock, self->log, self->socket, WORKER, HEARTBEAT, 0);
		}

		// The other case could be WORKER_STATE_REGISTERING.
		// self->heartbeat is used to keep send SERVICEREGREQ
		// every $self->heartbeat->_expire_time, so we don't
		// send anything just need this timer works.
	}
}

// Constructor.
//
// Parameters:
//   name - Worker name, two formats: 1. "aaa" or 2. "aaa.bbb".
//          Format 1 only provides service name, constructor will
//          append random string (8 bytes) to become worker name
//          (ex. "aaa.AKB48LOV").
//          Format 2 provides the full worker name. The string
//          before "." becomes the service name.
//   dispatcher - dispatcher address (ex. 172.17.153.190:5555)
//   oplong - callback function pointer to receive long operation task
//   opshort - callback function pointer to receive short operation task
// Return:
//   The address of created worker_t
worker_t *
worker_create(char *name, char *dispatcher, worker_oplong_fn *oplong, worker_opshort_fn *opshort)
{
	if(dispatcher == NULL || name == NULL) return NULL;

	worker_t *self = (worker_t *)zmalloc(sizeof(worker_t));
	zlog_init("log.conf");
	self->log = zlog_get_category("worker");
	if(!self->log)
		printf("zlog_get_category() failed\n");
	zlog_info(self->log, "Creating worker...");
	self->ctx = zctx_new();
	self->dispatcher = strdup(dispatcher);
	self->workerState = WORKER_STATE_UNREGISTER;
//	self->recvWaitTimeout = RECV_WAIT_TIMEOUT * ZMQ_POLL_MSEC;
	if(pthread_mutex_init(&self->sock_lock, NULL) != 0) {
		zlog_debug(self->log, "Failed to init send_lock");
	}
	self->heartbeat = heartbeat_create(15000, 3000, &_heartbeat_sendfn, self); // the keepalive must larger than recvWaitTimeout * 2, because zmq_poll base on recvWaitTimeout to wait for 2 sockets
	if(oplong)	self->oplong = oplong;
	if(opshort)	self->opshort = opshort;

	// Setup service name and worker name
	char *delimiter = strchr(name, '.');
	if((delimiter != NULL) && (*(delimiter + 1) != '\0')) {
		// $name is full worker name ("aaa.bbb")
		self->serviceName = strndup(name, delimiter - name);
		self->hostName = strdup(name);
	}
	else {
		// $name only has service name ("aaa" or "aaa."), append
		// random string to service name to form worker name
		int name_len = (!delimiter) ? strlen(name) : delimiter - name;
		self->serviceName = strndup(name, name_len);
		self->hostName = (char *)zmalloc(sizeof(char) * (name_len + 10));
		strncpy(self->hostName, name, name_len);
		self->hostName[name_len] = '.';
		rand_str(self->hostName + name_len + 1, 8);
	}
	zlog_info(self->log, "Create worker [%s] which provides [%s] service", self->hostName, self->serviceName);

	// Create IPC socket to communicate with thread. Due to socket can't
	// be shared between threads, so we let _worker_state_machine to
	// own the socket to dispatcher. It means only _worker_state_machine
	// can send_to/receive_from dispatcher. If any data wants to send from
	// non-_worker_state_machine, need to send through IPC socket and
	// forwarded by _worker_state_machine
	self->ipcAddress = (char *)zmalloc(sizeof(char) * (strlen(self->hostName) + 10));
	strncpy(self->ipcAddress, "inproc://", 9);
	strcat(self->ipcAddress + 9, self->hostName);
	self->ipcBindSocket = zsocket_new(self->ctx, ZMQ_PAIR);
	zsocket_bind(self->ipcBindSocket, self->ipcAddress);
	zlog_info(self->log, "Parent process bind to %s", self->ipcAddress);

	// Create _worker_state_machine thread
	pthread_create(&self->workerThread, NULL, &_worker_state_machine, (void *)self);

	return self;
}

// Destructor.
//
// Parameters:
//   self_p - address of created worker_t pointer
// Return:
//   None
void
worker_destroy(worker_t **self_p)
{
	if(self_p && *self_p)
	{
		worker_t *self = *self_p;
		zlog_info(self->log, "Destroying worker...");
		// Terminate _worker_state_machine by sending THREADCOMM_TERMINATE command through IPC
		zmsg_t *msg = zmsg_new();
		zmsg_pushstr(msg, THREADCOMM_TERMINATE);
		zmsg_send(&msg, self->ipcBindSocket);
		// Wait for thread terminate
		int *ret_val;
		int ret = pthread_join(self->workerThread, (void **)&ret_val);
		if(ret) { // Thread terminated with error
			zlog_error(self->log, "Failed to join worker thread termination, error code: %d", ret);
		}
//		else { // Thread terminated success, could read return value from ret_val
//			;
//		}
		heartbeat_destroy(&self->heartbeat);
		zctx_destroy (&self->ctx);
		pthread_mutex_destroy(&self->sock_lock);
		FREE(self->dispatcher);
		FREE(self->serviceName);
		FREE(self->hostName);
		FREE(self->ipcAddress);
		FREE(self);
		zlog_fini();
		*self_p = NULL;
	}
}

// Update task progress to dispatcher.
//
// Parameters:
//   self - created worker_t pointer
//   taskid - task ID in string format
//   percentage - Task progress. For normal case, should return
//                0~100. If any error happens, the possible
//                return value is TASK_FAIL. Any value larger
//                than 100 will be bounded to 100
// Return:
//   Return 0 means update is sent successfully. -1 means error
//   happens.
int
worker_update(worker_t *self, char *taskid, unsigned int percentage)
{
	if(self && taskid) {
		if(percentage > 100 &&
			percentage != TASK_FAIL) {
			zlog_debug(self->log, "Percentage value %u > 100, redirect to 100", percentage);
			percentage = 100;
		}

		char *buf = uitoa(percentage);
		if(buf) {
			zlog_info(self->log, "Update percentage %3u%s to task [%s]", percentage, (percentage != TASK_FAIL) ? "%%" : "", taskid);
			sendcmd(&self->sock_lock, self->log, self->socket, WORKER, TASKUPDATE, 2, taskid, buf);
			free(buf);
			return 0;
		}
		else
			zlog_error(self->log, "Failed to convert percentage %u to char array, nothing send", percentage);
	}
	return -1;
}

char *
worker_name(worker_t *self)
{
	if(self)
		return self->hostName;
	return NULL;
}

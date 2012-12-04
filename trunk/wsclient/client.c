#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option
#include "client.h"

#define DOLOG // !! DO NOT DEFINE DOLOG FOR REST API GATEWAY !!

struct _client_t {
    zctx_t*		ctx;				// Our context
    char*		hostName;			// Hostname assigned by constructor
    char*		dispatcher;			// Dispatcher address
    void*		socket;				// Socket to dispatcher
    uint64_t	recvWaitTimeout;	// Waiting timeout (millisecond) for message receiving

#ifdef DOLOG
    zlog_category_t*	log;
#endif
    pthread_mutex_t*	send_lock;
};

// Connect/reconnect to dispatcher
static void
_client_connect_to_dispatcher(client_t *self)
{
	if(self) {
		if(self->socket)
			zsocket_destroy(self->ctx, self->socket);
		if(!(self->socket = zsocket_new(self->ctx, ZMQ_DEALER))) {
#ifdef DOLOG
			zlog_error(self->log, "zsocket_new() return NULL, create socket failed");
#endif
			return;
		}
		FREE(self->hostName);
		self->hostName = (char *)zmalloc(sizeof(char) * 13);
		rand_str(self->hostName, 12);
		zmq_setsockopt(self->socket, ZMQ_IDENTITY, self->hostName, strlen(self->hostName));
		if(zmq_connect(self->socket, self->dispatcher) == 0) {
#ifdef DOLOG
			zlog_info(self->log, "Connecting to dispatcher at %s...", self->dispatcher);
#endif
		}
		else {
#ifdef DOLOG
			zlog_info(self->log, "Failed in zmq_connect(), error code = %d", errno);
#endif
		}
	}
}

// Receive message from dispatcher for $wait_t milliseconds, return command
// code and subsequent data. This method tries to wait for message base on
// $wait_t. If any legal message received will return IMMEDIATELY, it means
// waiting timeout mechanism should be controlled by caller.
static zmsg_t*
_client_recvcmd(client_t *self, uint64_t wait_t)
{
	zmsg_t *msg = NULL;
	if(self) {
		zmq_pollitem_t items[] = {{self->socket, 0, ZMQ_POLLIN, 0}};
		int rc = zmq_poll(items, 1, wait_t);
		if(rc == -1) return msg; // Interrupted

		if(items[0].revents & ZMQ_POLLIN) {
			msg = zmsg_recv(self->socket);
			if(!msg) return msg; // Interrupted

#ifdef DOLOG
			zlog_debug(self->log, "Receiving...");
			dumpzmsg(self->log, msg);
#endif

			// Empty frame:
			//   DEALER socket has to remove empty frame manually after receiving
			zframe_t *empty_frame = zmsg_pop(msg);
			assert(zframe_streq(empty_frame, ""));
			zframe_destroy(&empty_frame);
			// From frame:
			zframe_t *from_frame = zmsg_pop(msg);
			if(zframe_streq(from_frame, DISPATCHER)) {
				zframe_destroy(&from_frame);
				return msg; // This is the message we want
			}
#ifdef DOLOG
			else zlog_debug(self->log, "Message is not from DISPATCHER, ignore it");
#endif
			zframe_destroy(&from_frame);

			zmsg_destroy(&msg);
		}
	}
	return msg;
}

char *
_client_req(client_t *self, int type, int arg_length, ...)
{
	char *ret = NULL;
	if(!self || arg_length <= 0) return ret;

	// Prepare data frames that will send to dispatcher
	char **arg = (char **)zmalloc(sizeof(char *) * arg_length);
	if(arg) {
		va_list arguments;
		va_start(arguments, arg_length);
		for(int index = 0; index < arg_length; index++)
			arg[index] = va_arg(arguments, char *);
		va_end(arguments);
	}
	else return ret;

	// Based on type to send message to dispatcher
	switch(type)
	{
	case TASKCREATEREQ: // client_oplong
	case TASKDIRECTREQ: // client_opshort
		if(arg_length == 4) {
#ifdef DOLOG
			zlog_info(self->log, "Sending %s to dispatcher", (type == TASKCREATEREQ) ? "TASKCREATEREQ (oplong)" : "TASKDIRECTREQ (opshort)");
			zlog_info(self->log, "  token = %s",arg[0]);
			zlog_info(self->log, "  workers = %s",arg[1]);
			zlog_info(self->log, "  method = %s",arg[2]);
			zlog_info(self->log, "  data = %s",arg[3]);
			sendcmd(self->send_lock, self->log, self->socket, CLIENT, type, arg_length, arg[0], arg[1], arg[2], arg[3]);
#else
			sendcmd(self->send_lock, NULL, self->socket, CLIENT, type, arg_length, arg[0], arg[1], arg[2], arg[3]);
#endif
			if(type == TASKCREATEREQ)	type = TASKCREATEREP;
			else						type = TASKDIRECTREP;
		}
		else { FREE(arg); return ret; }
		break;
	case TASKQUERYREQ: // client_querytask
		if(arg_length == 1) {
#ifdef DOLOG
			zlog_info(self->log, "Sending TASKQUERYREQ for task ID [%s] to dispatcher", arg[0]);
			sendcmd(self->send_lock, self->log, self->socket, CLIENT, type, arg_length, arg[0]);
#else
			sendcmd(self->send_lock, NULL, self->socket, CLIENT, type, arg_length, arg[0]);
#endif
			type = TASKQUERYREP;
		}
		else { FREE(arg); return ret; }
		break;
	default: { FREE(arg); return ret; }
	}

	// Start to wait for return message
	timeout_t *t = timeout_create(self->recvWaitTimeout * ZMQ_POLL_MSEC);
	uint64_t remain = timeout_remain(t);
	zmsg_t *msg = NULL;
	char *cmd;
	int quit = 0;
	while(!quit) {
		if((msg = _client_recvcmd(self, remain))) {
			if((cmd = zmsg_popstr(msg))) {
				if(cmd_payload2code(cmd) == type) {

					switch(type)
					{
					case TASKCREATEREP: // client_oplong
#ifdef DOLOG
						if(is_zmsg_size_enough(self->log, msg, 3)) {
#else
						if(is_zmsg_size_enough(NULL, msg, 3)) {
#endif
							char *token_ret = zmsg_popstr(msg);
							char *taskid = zmsg_popstr(msg);
							char *status = zmsg_popstr(msg);

							if(token_ret && !strcmp(arg[0], token_ret)) {
								if(stat_payload2code(status) == S_OK) {
									ret = strdup(taskid);
#ifdef DOLOG
									zlog_info(self->log, "Task creation success, task ID: [%s]", ret);
#endif
								}
#ifdef DOLOG
								else zlog_info(self->log, "Dispatcher return fail, error code = %s", stat_code2str(stat_payload2code(status)));
#endif
								quit = 1;
							}
#ifdef DOLOG
							else {
								zlog_debug(self->log, "Recevied token not match, ignore it");
								zlog_debug(self->log, "  token sent [%s]", arg[0]);
								zlog_debug(self->log, "  token recv [%s]", token_ret ? token_ret : "empty token");
							}
#endif

							FREE(token_ret);
							FREE(taskid);
							FREE(status);
						}
						break;

					case TASKDIRECTREP: // client_opshort
#ifdef DOLOG
						if(is_zmsg_size_enough(self->log, msg, 2)) {
#else
						if(is_zmsg_size_enough(NULL, msg, 2)) {
#endif
							char *token_ret = zmsg_popstr(msg);
							char *data = zmsg_popstr(msg);

							if(token_ret && !strcmp(arg[0], token_ret)) {
								if(data && strcmp(data, "")) {
									ret = strdup(data);
#ifdef DOLOG
									zlog_info(self->log, "opshort got \"%s\" back", ret);
#endif
								}
#ifdef DOLOG
								else zlog_info(self->log, "opshort got empty string means error happens, return NULL to caller");
#endif
								quit = 1;
							}
#ifdef DOLOG
							else {
								zlog_debug(self->log, "Received token not match, ignore it");
								zlog_debug(self->log, "  token sent [%s]", arg[0]);
								zlog_debug(self->log, "  token recv [%s]", token_ret ? token_ret : "empty token");
							}
#endif

							FREE(token_ret);
							FREE(data);
						}
						break;

					case TASKQUERYREP: // client_querytask
#ifdef DOLOG
						if(is_zmsg_size_enough(self->log, msg, 2)) {
#else
						if(is_zmsg_size_enough(NULL, msg, 2)) {
#endif
							char *taskid_ret = zmsg_popstr(msg);
							char *percentage = zmsg_popstr(msg);

							if(taskid_ret && !strcmp(arg[0], taskid_ret)) {
								if(percentage) {
									ret = strdup(percentage);
#ifdef DOLOG
									zlog_info(self->log, "querytask got \"%s\" back", ret);
#endif
								}
#ifdef DOLOG
								else zlog_info(self->log, "querytask return null string");
#endif
								quit = 1;
							}
#ifdef DOLOG
							else {
								zlog_debug(self->log, "Received task ID not match, ignore it");
								zlog_debug(self->log, "  taskID sent [%s]", arg[0]);
								zlog_debug(self->log, "  taskID recv [%s]", taskid_ret ? taskid_ret : "empty token");
							}
#endif

							FREE(taskid_ret);
							FREE(percentage);
						}
						break;

					default: break; // shouldn't arrive here
					}
				}
#ifdef DOLOG
				else zlog_info(self->log, "Wrong command [%s], should be [%s]", cmd_code2str(cmd_payload2code(cmd)), cmd_code2str(type));
#endif
				free(cmd);
			}
#ifdef DOLOG
			else zlog_info(self->log, "Message received but no cmd inside");
#endif
			zmsg_destroy(&msg);
		}
#ifdef DOLOG
		else zlog_info(self->log, "No message received");
#endif

		if(!quit) {
			remain = timeout_remain(t);
			if(remain > 0)	{
#ifdef DOLOG
				zlog_info(self->log, "Still %u milliseconds remain, go on receiving", remain);
#endif
			}
			else { // timeout'd, reconnect to dispatcher
				_client_connect_to_dispatcher(self);
				quit = 1;

				if(type == TASKQUERYREP) {
					ret = strdup(TASK_RECV_TIMEOUT_STR);
				}
			}
		}
	}
	timeout_destroy(&t);
	free(arg);
	return ret;
}

// Constructor.
//
// Parameters:
//   dispatcher - dispatcher address, ex: tcp://172.17.153.190:5555
// Return:
//   The address of created client_t
client_t *
client_create (char *dispatcher)
{
    if(!dispatcher)	return NULL;

    client_t *self = (client_t *)zmalloc(sizeof(client_t));
#ifdef DOLOG
    zlog_init("/etc/wslog.conf");
    self->log = zlog_get_category("client");
	if(!self->log)
		printf("zlog_get_category() failed\n");
	zlog_info(self->log, "Creating client...");
#endif
    self->ctx = zctx_new ();
    //self->hostName = (char *)zmalloc(sizeof(char) * 13);
	//rand_str(self->hostName, 12);
    self->dispatcher = strdup(dispatcher);
    self->recvWaitTimeout = RECV_WAIT_TIMEOUT;
    self->send_lock = (pthread_mutex_t *)zmalloc(sizeof(pthread_mutex_t));
	if(self->send_lock) {
		if(pthread_mutex_init(self->send_lock, NULL) != 0) {
#ifdef DOLOG
			zlog_debug(self->log, "Failed to init send_lock");
#endif
			free(self->send_lock);
		}
	}
#ifdef DOLOG
	else
		zlog_debug(self->log, "Failed to allocate memory for send_lock");
#endif
	_client_connect_to_dispatcher(self);
    return self;
}

// Destructor.
//
// Parameters:
//   self_p - address of created client_t pointer
// Return:
//   None
void
client_destroy(client_t **self_p)
{
    if(self_p && *self_p) {
    	client_t *self = *self_p;
        zctx_destroy(&self->ctx);
        if(self->send_lock) {
        	pthread_mutex_destroy(self->send_lock);
        	free(self->send_lock);
        }
        FREE(self->dispatcher);
        FREE(self->hostName);
        FREE(self);
#ifdef DOLOG
        zlog_fini();
#endif
        *self_p = NULL;
    }
}

// To send out a long operation task and get task ID back. The
// method will be blocked for RECV_WAIT_TIMEOUT milliseconds
// at most if no message got.
//
// Parameters:
//   self - created client_t pointer
//   workers - task send to a service or specific worker. For
//             example, "iscsi" is a service name, "iscsi.1" is
//             to specify a receiver
//   method - task's method name
//   data - task's data for method tor process
// Return:
//   Task ID in string format (UUID string format, 36 bytes length).
//   NULL means error happens. Please remember to free when no
//   more use.
char *
client_oplong(client_t *self, char *workers, char *method, char *data)
{
	char *ret_id = NULL;
	if(!self || !workers || !method) return ret_id;

	char *token = gen_uuid_str();
	if(token) {
		ret_id = _client_req(self, TASKCREATEREQ, 4, token, workers, method, data);
		free(token);
	}
	return ret_id;
}

// To send out a short operation task and get result back. The
// method will be blocked for RECV_WAIT_TIMEOUT milliseconds
// at most if no message got.
//
// Parameters:
//   self - created client_t pointer
//   workers - task send to a service or specific worker. For
//             example, "iscsi" is a service name, "iscsi.1" is
//             to specify a receiver
//   method - task's method name
//   data - task's data for method tor process
// Return:
//   Result in string format. NULL means error happens. Please
//   remember to free when no more use.
char *
client_opshort(client_t *self, char *workers, char *method, char *data)
{
	char *ret_data = NULL;
	if(!self || !workers || !method) return ret_data;

	char *token = gen_uuid_str();
	if(token) {
		ret_data = _client_req(self, TASKDIRECTREQ, 4, token, workers, method, data);
		free(token);
	}
	return ret_data;
}

// Query task progress by task ID. The method will be blocked
// for RECV_WAIT_TIMEOUT milliseconds at most if no message got.
//
// Parameters:
//   self - created client_t pointer
//   taskid - task ID in string format
// Return:
//   Task progress. If there is no worker available, the return
//   value is TASK_DISPATCHING. For normal case, it should return
//   0~100. If any error happens, the possible return values are
//   TASK_FAIL/TASK_NOTFOUND
unsigned int
client_querytask(client_t *self, char *taskid)
{
	unsigned int ret_percentage = TASK_FAIL;
	if(self && taskid) {
		char *percentage = _client_req(self, TASKQUERYREQ, 1, taskid);
		if(percentage) {
			ret_percentage = atoi(percentage);
			if(ret_percentage > 100 &&
				ret_percentage != TASK_DISPATCHING &&
				ret_percentage != TASK_RECV_TIMEOUT &&
				ret_percentage != TASK_FAIL &&
				ret_percentage != TASK_NOTFOUND)
				ret_percentage = 100;
			else if(ret_percentage < 0) // Assume negative means error happens
				ret_percentage = TASK_FAIL;
			free(percentage);
		}
	}
	return ret_percentage;
}


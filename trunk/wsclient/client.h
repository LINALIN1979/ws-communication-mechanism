#ifndef CLIENT_H_
#define CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../wscommon/protocol.h"
#include "../wscommon/utils.h"

// How long will wait for a message (in millisecond)
#define RECV_WAIT_TIMEOUT	10000

//  Opaque class structure
typedef struct _client_t client_t;

// Constructor.
//
// Parameters:
//   dispatcher - dispatcher address, ex: tcp://172.17.153.190:5555
// Return:
//   The address of created client_t
client_t*		client_create(char *dispatcher);

// Destructor.
//
// Parameters:
//   self_p - address of created client_t pointer
// Return:
//   None
void			client_destroy(client_t **self_p);

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
//   Task ID in string format (UUID string format, 36 bytes length)
//   NULL means error happens.
char*			client_oplong(client_t *self, char *workers, char *method, char *data);

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
//   Result in string format. NULL means error happens
char*			client_opshort(client_t *self, char *workers, char *method, char *data);

// Return value of client_querytask. Normal case should return
// task percentage 0~100.
#define	TASK_DISPATCHING	901
#define TASK_FAIL			904
#define TASK_NOTFOUND		402

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
unsigned int	client_querytask(client_t *self, char *taskid);


#ifdef __cplusplus
}
#endif

#endif /* CLIENT_H_ */

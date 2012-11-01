#ifndef WORKER_H_
#define WORKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "../wscommon/protocol.h"
#include "../wscommon/utils.h"

// Opaque class structure
typedef struct _worker_t worker_t;

// Callback for Long Operation
#define OPLONG_ACCEPT	0
#define OPLONG_REJECT	1
typedef int		(worker_oplong_fn) (char *taskid, char *method, char *data); // Please return OPLONG_ACCEPT/OPLONG_REJECT

// Callback for Short Operation, return result in char array
typedef char*	(worker_opshort_fn)(char *method, char *data);


// Function declarations

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
worker_t*	worker_create(char *name, char *dispatcher, worker_oplong_fn *oplong, worker_opshort_fn *opshort);

// Destructor.
//
// Parameters:
//   self_p - address of created worker_t pointer
// Return:
//   None
void		worker_destroy(worker_t **self_p);

// Update task progress to dispatcher.
//
// Parameters:
//   self - created worker_t pointer
//   taskid - task ID in string format
//   percentage - Task progress. For normal case, should return
//                0~100. If any error happens, the possible
//                return value is TASK_FAIL. Any value larger
//                than 100 will be bunded to 100
// Return:
//   Return 0 means update is sent successfully. -1 means error
//   happens.
int			worker_update(worker_t *self, char *taskid, unsigned int percentage);

// Get worker's name
char*		worker_name(worker_t *self);

// Special task status percentage of worker_update. Normal case should return
// task percentage 0~100.
//#define	TASK_DISPATCHING	901 // Useless to worker
#define TASK_FAIL			904
//#define TASK_NOTFOUND		402 // Useless to worker


#ifdef __cplusplus
}
#endif

#endif /* WORKER_H_ */

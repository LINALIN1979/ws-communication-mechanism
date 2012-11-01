#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option

#include "../wsworker/worker.h"
#include <signal.h>

worker_t *session = NULL;

// fake update
void *sleep_then_return(void *ptr)
{
	if(session) {
		char *taskid = (char *)ptr;
		for(unsigned int status = 10; status <= 100; status += 10) {
			zclock_sleep(700);
			worker_update(session, taskid, status); // percentage
		}
		free(taskid);
	}
	return NULL;
}

int oplong(char *taskid, char *method, char *data)
{
	pthread_t tmp;
	if(taskid)	printf("oplong: taskid = %s\n", taskid);
	if(method)	printf("oplong: method = %s\n", method);
	if(data)	printf("oplong: data = %s\n", data);
	char *task = strdup(taskid);
	pthread_create(&tmp, NULL, &sleep_then_return, (void *)task);
	return OPLONG_ACCEPT; // or OPLONG_REJECT
}

char* opshort(char *method, char *data)
{
	if(method)	printf("opshort: method = %s\n", method);
	if(data)	printf("opshort: data = %s\n", data);

	if(!strcmp(data, "what time is it?"))
	{
		time_t timer;
	    static char buffer[25];
	    struct tm* tm_info;
	    time(&timer);
	    tm_info = localtime(&timer);

	    strftime(buffer, 25, "%Y:%m:%d%H:%M:%S", tm_info);
	    //free(tm_info); <-- tm_info can't be free()
	    return buffer;
	}
	return NULL;
}

void my_handler(int s)
{
	printf("SIGINT received, terminate the worker.\n");
	worker_destroy(&session);
	exit(0);
}

int main (int argc, char *argv [])
{
	if(argc != 3) {
		printf("Usage: %s tcp://<dispatcher address>:<port> <your hostname>\n", argv[0]);
		return 0;
	}

	session = worker_create(argv[2], argv[1], &oplong, &opshort);
	if(!session) {
		printf("WTF! why there is no worker created?\n");
		return -1;
	}
	else {
		struct sigaction sigIntHandler;
		sigIntHandler.sa_handler = &my_handler;
		sigemptyset(&sigIntHandler.sa_mask);
		sigIntHandler.sa_flags = 0;
		sigaction(SIGINT, &sigIntHandler, NULL);
	}

	while(1) {
		zclock_sleep(1000 * 60);
	}

//	zclock_sleep(15000);
	worker_destroy(&session);

    return 0;
}

#define _GNU_SOURCE	// Due to strdup is not standard C function in string.h, add this
					// definition to make compiler see strdup from string.h. Or to
					// remove "-ansi" from compiler option

#include "../wsworker/worker.h"
#include "../wsclient/client.h"
#include <signal.h>

#define DO_CLIENT 	0

worker_t 		*worker = NULL;
threadpool_t	*threads = NULL;
#if DO_CLIENT
client_t 		*client = NULL;
#endif

// fake update
//void *
void
sleep_then_return(void *ptr)
{
	if(worker) {
		char *taskid = (char *)ptr;
		if(taskid) {
			for(unsigned int status = 10; status <= 100; status += 10) {
				zclock_sleep(700);
				worker_update(worker, taskid, status); // percentage
			}
			free(taskid);
		}
	}
	//return NULL;
}

#if DO_CLIENT
void
fake_client_request(void *ptr)
{
	zclock_sleep(5000);
	printf("oplong: ");
	char *taskid = client_oplong(client, "time.1", "method:time", "what time is it?");
	if(taskid) {
		int quit = 0;
		unsigned int percentage;
		while(!quit) {
			zclock_sleep(1000);
			percentage = client_querytask(client, taskid);
			switch(percentage)
			{
			case 100:
				printf(" Task done!\n");
				quit = 1;
				break;

			case TASK_NOTFOUND:
				printf(" Task not found!\n");
				quit = 1;
				break;

			case TASK_FAIL:
				printf(" Task failed!\n");
				quit = 1;
				break;

			default:
				printf(" %d\n", percentage);
				break;
			}
		}
		free(taskid);
	}

	printf("opshort: ");
	char *ret = client_opshort(client, "time", "method:time", "what time is it?");
	if(ret)	{ printf("%s\n", ret); free(ret); }
	else	{ printf("fail\n"); }
}
#endif

int oplong(char *taskid, char *method, char *data)
{
	//pthread_t tmp;
	if(taskid)	printf("oplong: taskid = %s\n", taskid);
	if(method)	printf("oplong: method = %s\n", method);
	if(data)	printf("oplong: data = %s\n", data);
	char *task = strdup(taskid);
	//pthread_create(&tmp, NULL, &sleep_then_return, (void *)task);
	if(threads) {
		if(threadpool_add(threads, &sleep_then_return, (void *)task, 0) != 0) {
			printf("threadpool_add failed, task ID = %s\n", task);
		}
	}
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

void destroy()
{
	if(threads)
		threadpool_destroy(threads, 0);
	printf(" Terminate the worker\n");
	worker_destroy(&worker);
#if DO_CLIENT
	printf(" Terminate the client\n");
	client_destroy(&client);
#endif
	printf("Terminated...\n");
}

void my_handler(int s)
{
	printf("SIGINT received...\n");
	destroy();
	exit(0);
}

int main (int argc, char *argv [])
{
	if(argc != 3) {
		printf("Usage: %s tcp://<dispatcher address>:<port> <your hostname>\n", argv[0]);
		return 0;
	}

	threads = threadpool_create(4, 24, 0);

	worker = worker_create(argv[2], argv[1], &oplong, &opshort);
	if(!worker) {
		printf("WTF! why there is no worker created?\n");
		goto err;
	}
#if DO_CLIENT
	client = client_create(argv[1]);
	if(!client) {
		printf("WTF! why there is no client created?\n");
		goto err;
	}
#endif

	struct sigaction sigIntHandler;
	sigIntHandler.sa_handler = &my_handler;
	sigemptyset(&sigIntHandler.sa_mask);
	sigIntHandler.sa_flags = 0;
	sigaction(SIGINT, &sigIntHandler, NULL);

#if DO_CLIENT
	if(threads) {
		if(threadpool_add(threads, &fake_client_request, NULL, 0) != 0) {
			printf("Create fake_client_request failed\n");
		}
	}
	else goto err;
#endif

	while(1) {
		zclock_sleep(1000 * 60);
	}
	return 0;

err:
	destroy();
    return -1;
}

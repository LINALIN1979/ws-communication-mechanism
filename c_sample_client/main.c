#include "../wsclient/client.h"
#include <signal.h>

client_t *session = NULL;

void
fake_client_request(void *ptr)
{
	printf("oplong: ");
	char *taskid = client_oplong(session, "time.1", "method:time", "what time is it?");
	if(taskid) {
		int quit = 0;
		unsigned int percentage;
		while(!quit) {
			zclock_sleep(1000);
			percentage = client_querytask(session, taskid);
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
	char *ret = client_opshort(session, "time", "method:time", "what time is it?");
	if(ret)	{ printf("%s\n", ret); free(ret); }
	else	{ printf("fail\n"); }
}

void destroy()
{
	client_destroy(&session);
	printf("Terminated...\n");
}

void my_handler(int s)
{
	printf("SIGINT received, terminate the client.\n");
	destroy();
	exit(0);
}

int main(int argc, char **argv)
{
	if(argc != 2)
	{
		printf("Usage: %s tcp://<dispatcher IP>:<port>\n", argv[0]);
		return 0;
	}

	session = client_create(argv[1]);
	if(!session) {
		printf("WTF! why there is no client created?\n");
		return -1;
	}

	struct sigaction sigIntHandler;
	sigIntHandler.sa_handler = &my_handler;
	sigemptyset(&sigIntHandler.sa_mask);
	sigIntHandler.sa_flags = 0;
	sigaction(SIGINT, &sigIntHandler, NULL);

	while(1) {
		fake_client_request(session);
		zclock_sleep(1000);
	}

	destroy();
	return 0;
}

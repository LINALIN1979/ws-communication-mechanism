#ifndef PROTOCOL_H__
#define PROTOCOL_H__

#ifdef __cplusplus
extern "C" {
#endif


// Payload to fill in as sender
#define DISPATCHER	"WSD"
#define CLIENT		"WSC"
#define WORKER		"WSW"

// Commands, must start from 1 and the last command is UNKNOWN
#define SERVICEREGREQ	1
#define	SERVICEREGREP	2
#define TASKCREATEREQ	3
#define TASKCREATEREP	4
#define TASKDISREQ		5
#define TASKDISREP		6
#define TASKQUERYREQ	7
#define	TASKQUERYREP	8
#define DISCONNECT		9
#define TASKUPDATE		10
#define TASKDIRECTREQ	11
#define TASKDIRECTREP	12
#define HEARTBEAT		1991
#define UNKNOWN			9999 // UNKNOWN command is just for program process, won't be send in packet

char*	cmd_code2payload(int code);
char*	cmd_code2str(int code);
int		cmd_payload2code(char *cmd);


// Status codes for REP command
//   Success codes
#define S_OK			100
//#define S_DISPATCHING	101
//#define S_WIP			102
//#define S_DONE		103
//   Failed codes
#define S_FAIL			400
#define S_EXISTED		401
#define S_NOTFOUND		402
#define S_UNKNOWNSERV	403
#define S_UNKNOWNWORKER 404
#define S_NOWORKER		405
// Special code for task status, normal case for task status should be 0~100
// ps. do not conflict with failed codes
#define DISPATCHING		901
#define FAIL			904
#define S_UNKNOWN		999 // S_UNKNOWN command is just for program process, won't be used in packet or task status

char*	stat_code2payload(int code);
char*	stat_code2str(int code);
int		stat_payload2code(char *cmd);


#ifdef __cplusplus
}
#endif

#endif /* PROTOCOL_H_ */

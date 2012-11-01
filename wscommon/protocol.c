#include "protocol.h"
#include <string.h>

// Structure for command and status code definitions
typedef struct {
	int		code;			// command code for program process
	char	*payload;		// command data to write into packet
	char	*str;			// command string for debugging purpose
} wscc_t;

static wscc_t COMMANDS[] = {
		{SERVICEREGREQ,		"001",	"SERVICE_REG_REQ"},
		{SERVICEREGREP,		"002",	"SERVICE_REG_REP"},
		{TASKCREATEREQ,		"003",	"TASK_CREATE_REQ"},
		{TASKCREATEREP,		"004",	"TASK_CREATE_REP"},
		{TASKDISREQ,		"005",	"TASK_DISPATCH_REQ"},
		{TASKDISREP,		"006",	"TASK_DISPATCH_REP"},
		{TASKQUERYREQ,		"007",	"TASK_QUERY_REQ"},
		{TASKQUERYREP,		"008",	"TASK_QUERY_REP"},
		{DISCONNECT,		"009",	"DISCONNECT"},
		{TASKUPDATE,		"010",	"TASK_STATE_UPDATE"},
		{TASKDIRECTREQ,		"011",	"TASK_DIRECT_REQ"},
		{TASKDIRECTREP,		"012",	"TASK_DIRECT_REP"},
		{HEARTBEAT,			"1991",	"HEARTBEAT"},

		{UNKNOWN,			NULL,	"UNKNOWN"},
};
static int COMMANDS_FINAL_POS = (int)(sizeof(COMMANDS)/sizeof(wscc_t)) - 1; // The final position of UNKNOWN

static wscc_t STATCODES[] = {
		{S_OK,				"100",	"OK"},
//		{S_DISPATCHING,		"101",	"DISPATCHING"},
//		{S_WIP,				"102",	"WORK_IN_PROGRESS"},
//		{S_DONE,			"103",	"DONE"},

		{S_FAIL,			"400",	"FAILED"},
		{S_EXISTED,			"401",	"ALREADY_EXISTED"},
		{S_NOTFOUND,		"402",	"NOT_FOUND"},
		{S_UNKNOWNSERV,		"403",	"UNKNOWN_SERVICE"},
		{S_UNKNOWNWORKER,	"404",	"UNKNOWN_WORKER"},
		{S_NOWORKER,		"405",	"NO_WORKER_AVAILABLE"},

		{S_UNKNOWN,			NULL,	"UNKNOWN"},
};
static int STATCODES_FINAL_POS = (int)(sizeof(STATCODES)/sizeof(wscc_t)) - 1; // The final position of S_UNKNOWN

// Translate code (int) to other formats base on $type and
// $format
//   type = 1: command
//   type = 2: status code
//   format = 1: payload (for packet assembly)
//   format = 2: string (for debug)
static char *
_code2something(int code, int type, int format)
{
	wscc_t	*target;
	int		target_len;
	switch(type)
	{
	case 1:
		target = COMMANDS;
		target_len = COMMANDS_FINAL_POS - 1; // -1 to make index won't larger than COMMANDS_FINAL_POS
		break;
	case 2:
		target = STATCODES;
		target_len = STATCODES_FINAL_POS - 1; // -1 to make index won't larger than STATCODES_FINAL_POS
		break;
	default:	return NULL;
	}

	int index = 0;
	for(; index < target_len; index++)
	{
		if(target[index].code == code) break; // match
	}

	switch(format)
	{
	case 1:		return target[index].payload;	// payload
	case 2:		return target[index].str;		// string
	default:	return NULL;					// ...supposed won't reach here
	}
}

// Translate payload (char*) to command/status code (int)
//   type = 1: command
//   type = 2: status code
// Return -1 means $cmd is NULL or $type error
static int
_payload2code(char *cmd, int type)
{
	if(cmd == NULL) return -1;

	wscc_t	*target;
	int		target_len;
	switch(type)
	{
	case 1:
		target = COMMANDS;
		target_len = COMMANDS_FINAL_POS - 1; // -1 to make index won't larger than COMMANDS_FINAL_POS
		break;
	case 2:
		target = STATCODES;
		target_len = STATCODES_FINAL_POS - 1; // -1 to make index won't larger than STATCODES_FINAL_POS
		break;
	default:	return -1;
	}

	int cmd_len = strlen(cmd);
	if(cmd_len < 1) return -1; // nothing to match for empty string

	int index = 0;
	for(; index < target_len; index++)
	{
		if((cmd_len == strlen(target[index].payload)) &&
				(!memcmp(cmd, target[index].payload, cmd_len))) break; // match
	}
	return target[index].code;
}

// Translate command code (int) to payload (char*)
char *
cmd_code2payload(int code)
{
	return _code2something(code, 1, 1);
}

// Translate command code (int) to str (char*)
char *
cmd_code2str(int code)
{
	return _code2something(code, 1, 2);
}

// Translate payload (char*) to command code (int)
int
cmd_payload2code(char *cmd)
{
	return _payload2code(cmd, 1);
}

// Translate status code (int) to payload (char*)
char *
stat_code2payload(int code)
{
	return _code2something(code, 2, 1);
}

// Translate status code (int) to str (char*)
char *
stat_code2str(int code)
{
	return _code2something(code, 2, 2);
}

// Translate payload (char*) to command code (int)
int
stat_payload2code(char *cmd)
{
	return _payload2code(cmd, 2);
}


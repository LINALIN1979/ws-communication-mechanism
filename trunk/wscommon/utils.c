#include "utils.h"
#include "protocol.h"
#include <uuid/uuid.h>

static uint32_t crc32_tab[] = {
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
	0xe963a535, 0x9e6495a3,	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
	0xf3b97148, 0x84be41de,	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,	0x14015c4f, 0x63066cd9,
	0xfa0f3d63, 0x8d080df5,	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,	0x35b5a8fa, 0x42b2986c,
	0xdbbbc9d6, 0xacbcf940,	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
	0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,	0x76dc4190, 0x01db7106,
	0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
	0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
	0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
	0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
	0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
	0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
	0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
	0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
	0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
	0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
	0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
	0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
	0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
	0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
	0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
	0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

uint32_t
crc32(uint32_t crc, const void *buf, size_t size)
{
	const uint8_t *p;

	p = buf;
	crc = crc ^ ~0U;

	while (size--)
		crc = crc32_tab[(crc ^ *p++) & 0xFF] ^ (crc >> 8);

	return crc ^ ~0U;
}

// Fill in random string ([0-9A-Z]) to $dst, assign generated string
// length in $size. Please be careful to use, if $size is N, $dst
// length is at least N+1 (for terminate char '\0').
void
rand_str(char *dst, int size)
{
	static const char text[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	static int firstRun = 1;
	static int text_length = 0;

	if(firstRun) {
		srand(zclock_time());
		text_length = sizeof(text) - 1;
		firstRun = 0;
	}

	if(dst) {
		for(int index = 0; index < size; index++)
			dst[index] = text[rand() % text_length];
		dst[size] = '\0';
	}
}

// Genereate UUID string, remember to free when no more use
char *
gen_uuid_str()
{
	char *uuid_str = (char *)zmalloc(sizeof(char) * 37);
	uuid_t new_uuid;
	uuid_generate(new_uuid);
	uuid_unparse_upper(new_uuid, uuid_str);
	uuid_str[36] = '\0';
	return uuid_str;
}

// Dump zmsg content by frame. Only output the first 10 frames
// for performance concern.
void
dumpzmsg(zlog_category_t *log, zmsg_t *self)
{
	size_t buf_len = 512;
	char *buf = (char *)zmalloc(sizeof(char) * buf_len);

	zframe_t *frame = zmsg_first(self);
	int frame_nbr = 0;
	int next_write = 0;
	while(frame && frame_nbr++ < 10) {
		byte *data = zframe_data(frame);
		size_t size = zframe_size(frame);

		// Frame is binary format or not?
		int is_bin = 0;
		uint char_nbr;
		for(char_nbr = 0; char_nbr < size; char_nbr++) {
			if(data[char_nbr] < 9 || data[char_nbr] > 127) {
				is_bin = 1;
				break;
			}
		}

		// Prepare memory for output
		size_t frame_total_length = 7 + (is_bin ? size*2 : size+2); // is_bin  - [%03d ...], "["+"%03d"+" "+"]"+"\0" = 7
																	// !is_bin - [%03d "..."], "["+"%03d"+" "+"\""+"\""+"]"+"\0" = 9
		if((buf_len - next_write) < frame_total_length) {
			buf_len = next_write + frame_total_length;
			buf = realloc(buf, buf_len);
		}

		if(buf) {
			// Output starting of frame
			if(!is_bin) { snprintf(buf + next_write, 7, "[%03d \"", (int)size); next_write += 6; }
			else		{ snprintf(buf + next_write, 6, "[%03d ", (int)size);   next_write += 5; }
			// Output content
			for(char_nbr = 0; char_nbr < size; char_nbr++) {
				if(is_bin) {
					snprintf(buf + next_write, 3, "%02X", (unsigned char)data[char_nbr]);
					next_write += 2;
				}
				else {
					snprintf(buf + next_write, 2, "%c", data[char_nbr]);
					next_write++;
				}
			}
			// Output ending of frame
			if(!is_bin) { snprintf(buf + next_write, 3, "\"]");	next_write += 2; }
			else		{ snprintf(buf + next_write, 2, "]");   next_write++; }
		}
		else {
			zlog_error(log, "dumpzmsg failed, output buffer became empty");
			break;
		}

		frame = zmsg_next(self);
	}

	if(buf) {
		if(log)	zlog_debug(log, "%s", buf);
		else	printf("%s\n", buf);
		free(buf);
	}
}

// Send message through $socket
//
// Parameters:
//   socket - zeromq socket descriptor
//   from - sender type (DISPATCHER, WORKER, CLIENT)
//   command - command type (ex. SERVICEREGREQ, TASKCREATEREP)
//   arg_length - the arguments length of subsequent inputs
//   ... - char arrays to compose multi-part messages to send
void
sendcmd(pthread_mutex_t *lock, zlog_category_t* log, void *socket, char *from, int command, int arg_length, ...)
{
	if(!socket) return;

//	zlog_debug(log, "sendcmd: zmsg_new() start");
	zmsg_t *msg = zmsg_new();
//	zlog_debug(log, "sendcmd: zmsg_new() done");

	// Data frames:
	if(arg_length > 0) {
		va_list arguments;
		va_start(arguments, arg_length);

		char *tmp;
		for(int index = 0; index < arg_length; index++) {
			tmp = va_arg(arguments, char *);
			if(tmp)	{
//				zlog_debug(log, "sendcmd: addstr \"%s\" start", tmp);
				zmsg_addstr(msg, tmp);
//				zlog_debug(log, "sendcmd: addstr \"%s\" done", tmp);
			}
			//else	zmsg_addstr(msg, "");
		}
		va_end(arguments);
	}
	// Command frame:
//	zlog_debug(log, "sendcmd: pushstr start");
	zmsg_pushstr(msg, cmd_code2payload(command));
//	zlog_debug(log, "sendcmd: pushstr done");
	// From frame:
//	zlog_debug(log, "sendcmd: pushstr \"%s\" start", from);
	zmsg_pushstr(msg, from);
//	zlog_debug(log, "sendcmd: pushstr \"%s\" done", from);
	// Empty frame:
	//   DEALER socket has to add empty frame manually before sending
//	zlog_debug(log, "sendcmd: pushstr empty start");
	zmsg_pushstr(msg, "");
//	zlog_debug(log, "sendcmd: pushstr empty start");

	zlog_debug(log, "Sending...");
	dumpzmsg(log, msg);

	// lock mutex if any
	if(lock) {
		if(pthread_mutex_lock(lock) != 0) {
			zlog_error(log, "Unable to lock mutex in sendcmd(), do not send message");
			return;
		}
	}

//	zlog_debug(log, "sendcmd: zmsg_send() start");
	if(zmsg_send(&msg, socket))
		zlog_error(log, "Failed to send in zmsg_send(), error code = %d", errno);
//	zlog_debug(log, "sendcmd: zmsg_send() done");

	// unlock mutex if any
	if(lock) {
		if(pthread_mutex_unlock(lock) != 0) {
			zlog_error(log,"Failed to unlock mutex in sendcmd(), subsequent messages may be blocked");
			return;
		}
	}
}

// Convert unsigned integer (4bytes) to char array, please remember to free
char *
uitoa(unsigned int value)
{
	size_t buf_len = 16;
	char *buf = (char *)zmalloc(sizeof(char) * buf_len);

	while(buf) {
		int required = snprintf(buf, buf_len, "%u", value);
		if(required >= buf_len) {				// buf is not enough for percentage output, enlarge it
			buf_len = required + 1;
			buf = realloc(buf, buf_len);
		}
		else if(required < 0)	return NULL;	// snprintf error happens
		else					break;			// snprintf success
	}
	return buf;
}

// Convert uint64_t (8bytes) to char array, please remember to free
char *
ultoa(uint64_t value)
{
	size_t buf_len = 24;
	char *buf = (char *)zmalloc(sizeof(char) * buf_len);

	while(buf) {
		int required = snprintf(buf, buf_len, "%lu", value);
		if(required >= buf_len) {				// buf is not enough for percentage output, enlarge it
			buf_len = required + 1;
			buf = realloc(buf, buf_len);
		}
		else if(required < 0)	return NULL;	// snprintf error happens
		else					break;			// snprintf success
	}
	return buf;
}

uint64_t
atoul(char *src, size_t src_length, int base)
{
	uint64_t ret = 0;
//	printf("atoul: %s, length = %d\n", src, (int)src_length);
	if(src) {
		uint64_t multi = 1;
		char *tmp;
		for(int i = src_length - 1; i >= 0; i--) {
//			printf("[%d:%c]: %lu -> ", i, src[i], ret);
			tmp = src + i;

			switch(base) {
			case 8:
				// Number
				if     ((*tmp >= '0') && (*tmp <= '8'))	ret += (uint64_t)((*tmp - '0') * multi);
				// Unknow -> do nothing
				multi <<= 3;
				break;
			case 16:
				// Number
				if     ((*tmp >= '0') && (*tmp <= '9'))	ret += (uint64_t)((*tmp - '0') * multi);
				// a~f
				else if((*tmp >= 'a') && (*tmp <= 'f'))	ret += (uint64_t)((*tmp - 'a' + 10) * multi);
				// A~F
				else if((*tmp >= 'A') && (*tmp <= 'F'))	ret += (uint64_t)((*tmp - 'A' + 10) * multi);
				// Unknow -> do nothing
				multi <<= 4;
				break;
			case 10:
			default:
				// Number
				if     ((*tmp >= '0') && (*tmp <= '9'))	ret += (uint64_t)((*tmp - '0') * multi);
				// Unknow -> do nothing
				multi *= 10;
				break;
			}
//			printf("%lu\n", ret);
		}
	}
	return ret;
}

// Check zmsg_t size is larger than or equal to $size or not. Return 1 means
// yes, 0 means not enough and output error.
int
is_zmsg_size_enough(zlog_category_t* log, zmsg_t *msg, int size)
{
	if(!msg) {
		if(log)
			zlog_error(log, "is_zmsg_size_enough: msg ptr is NULL, can't get size");
		else
			printf("is_zmsg_size_enough: msg ptr is NULL, can't get size\n");
		return 0;
	}
	int msg_size = zmsg_size(msg);
	if(msg_size >= size) {
		return 1;
	}
	else {
		if(log)
			zlog_info(log, "is_zmsg_size_enough: msg size is %d which is smaller than %d", msg_size, size);
		else
			printf("is_zmsg_size_enough: msg size is %d which is smaller than %d\n", msg_size, size);
		return 0;
	}
}

struct _timeout_t{
	uint64_t	_old;
	uint64_t	_new;		// Next expiration time

	uint64_t	_interval;
};

int
timeout_serialize(timeout_t *self, serialize_t *buf)
{
	if(self && buf) {
		if(serialize_w_uint64(buf, self->_old) != 0)		goto timeout_serialize_err;
		if(serialize_w_uint64(buf, self->_new) != 0)		goto timeout_serialize_err;
		if(serialize_w_uint64(buf, self->_interval) != 0)	goto timeout_serialize_err;
		serialize_w_done(buf);
		return 0;
	}

timeout_serialize_err:
	return 1;
}

timeout_t *
timeout_deserialize(serialize_t *buf)
{
	timeout_t *self = NULL;
	if(buf) {
		if(serialize_r_prepare(buf) == 0) {
			self = (timeout_t *)zmalloc(sizeof(timeout_t));
			if(self) {
				self->_old = serialize_r_uint64(buf);
				self->_new = serialize_r_uint64(buf);
				self->_interval = serialize_r_uint64(buf);
			}
		}
	}
	return self;
}

// Create timeout_t object.
//
// Parameters:
//   The timeout interval in millisecond
// Return:
//   Created timeout_t object
timeout_t *
timeout_create(uint64_t interval_ms)
{
	timeout_t *ret = (timeout_t *)zmalloc(sizeof(timeout_t));
	if(ret) {
		ret->_interval = interval_ms;
		timeout_update(ret);
	}
	return ret;
}

timeout_t *
timeout_create_manually(uint64_t _old, uint64_t _new, uint64_t _interval)
{
	timeout_t *ret = (timeout_t *)zmalloc(sizeof(timeout_t));
	if(ret) {
		ret->_old = _old;
		ret->_new = _new;
		ret->_interval = _interval;
	}
	return ret;
}

// Return remaining time in millisecounds.
//
// Return:
//   0 means timeout'd, other return values mean remaining time
uint64_t
timeout_remain(timeout_t *self)
{
	if(self) {
		uint64_t now = zclock_time();
		// Not expired yet, return remaining time
		if(now < self->_new)
			return (self->_new - now);
	}
	return 0;
}

// Update timeout value, it will be expired in $self->_intervalu
// milliseconds from now on
void
timeout_update(timeout_t *self)
{
	if(self) {
		self->_old = zclock_time();
		self->_new = self->_old + self->_interval;
	}
}

void
timeout_destroy(timeout_t **self_p)
{
	if(self_p && *self_p) {
		timeout_t *self = *self_p;
		FREE(self);
		*self_p = NULL;
	}
}

void
timeout_print(timeout_t *self)
{
	if(self) {
		printf("  timeout_t->_old = %lu\n", self->_old);
		printf("  timeout_t->_new = %lu\n", self->_new);
		printf("  timeout_t->_interval = %lu\n", self->_interval);
	}
}

uint64_t
timeout_get_old(timeout_t *self)
{
	if(self)return self->_old;
	else	return 0;
}

void
timeout_set_old(timeout_t *self, uint64_t _old)
{
	if(self)self->_old = _old;
}

uint64_t
timeout_get_new(timeout_t *self)
{
	if(self)return self->_new;
	else	return 0;
}

void
timeout_set_new(timeout_t *self, uint64_t _new)
{
	if(self)self->_new = _new;
}

uint64_t
timeout_get_interval(timeout_t *self)
{
	if(self)return self->_interval;
	else	return 0;
}

void
timeout_set_interval(timeout_t *self, uint64_t _interval)
{
	if(self)self->_interval = _interval;
}

struct _heartbeat_t {
	uint64_t			_deadtime;	// The time (in ms) to judge peer dead or not
	uint64_t			_keepalive;	// The time between heartbeats

	timeout_t			*timeout;
	int					retries;

	heartbeat_send_fn	*send_fn;
	void				*send_param;
};

int
heartbeat_serialize(heartbeat_t *self, serialize_t *buf)
{
	if(self && buf) {
		if(serialize_w_uint64(buf, self->_deadtime) != 0)	goto heartbeat_serialize_err;
		if(serialize_w_uint64(buf, self->_keepalive) != 0)	goto heartbeat_serialize_err;
		serialize_t *buf2 = serialize_create();
		if(!buf2) goto heartbeat_serialize_err;
		else {
			if(timeout_serialize(self->timeout, buf2) != 0) {
				serialize_destroy(buf2);
				goto heartbeat_serialize_err;
			}
			if(serialize_w_serialize(buf, buf2) != 0) {
				serialize_destroy(buf2);
				goto heartbeat_serialize_err;
			}
			serialize_destroy(buf2);
		}
		if(serialize_w_uint64(buf, self->retries) != 0)		goto heartbeat_serialize_err;
		serialize_w_done(buf);
		return 0;
	}

heartbeat_serialize_err:
	return 1;
}

heartbeat_t *
heartbeat_deserialize(serialize_t *buf)
{
	heartbeat_t *self = NULL;
	if(buf) {
		if(serialize_r_prepare(buf) == 0) {
			self = (heartbeat_t *)zmalloc(sizeof(heartbeat_t));
			if(self) {
				self->_deadtime = serialize_r_uint64(buf);
				self->_keepalive = serialize_r_uint64(buf);
				self->timeout = timeout_deserialize(buf);
				if(self->timeout == NULL)	goto heartbeat_deserialize_err;
				self->retries = (int)serialize_r_uint64(buf);
			}
		}
	}
	return self;

heartbeat_deserialize_err:
	if(self)	heartbeat_destroy(&self);
	return NULL;
}

// Create heartbeat object.
//
// Parameters:
//   deadtime - if doesn't receive peer's heartbeat for $deadtime
//              (millisecond), it will be pronounced dead
//   keepalive - the time between heartbeats
//   send_fn - a callback function for caller to assign what to
//             do for sending heartbeat
//   send_param - parameter for send_fn callback
// Return:
//   Created heartbeat object
heartbeat_t *
heartbeat_create(uint64_t deadtime, uint64_t keepalive, heartbeat_send_fn *send_fn, void *send_param)
{
	heartbeat_t *self = (heartbeat_t *)zmalloc(sizeof(heartbeat_t));
	if(self) {
		self->_deadtime = deadtime;
		self->_keepalive = keepalive;

		self->send_fn = send_fn;
		self->send_param = send_param;
		heartbeat_reset_retries(self);
		self->timeout = timeout_create(self->_keepalive);
	}
	return self;
}

heartbeat_t *
heartbeat_create_manually(uint64_t deadtime, uint64_t keepalive,
		uint64_t timeout_old, uint64_t timeout_new, uint64_t timeout_interval, int retries,
		heartbeat_send_fn *send_fn, void *send_param)
{
	heartbeat_t *self = (heartbeat_t *)zmalloc(sizeof(heartbeat_t));
	if(self) {
		self->_deadtime = deadtime;
		self->_keepalive = keepalive;

		self->timeout = timeout_create_manually(timeout_old, timeout_new, timeout_interval);
		self->retries = retries;

		self->send_fn = send_fn;
		self->send_param = send_param;
	}
	return self;
}

// Do heartbeat check, 1 means still alive, 0 means dead. During
// the check, if it is time to send heartbeat, callback function
// will be called.
//
// Return:
//   1 means alive, 0 means dead.
int
heartbeat_check(heartbeat_t *self)
{
	if(self) {
		// Timeout'd
		if(timeout_remain(self->timeout) == 0) {
			// Retry remains, go retry and update timeout value
			if(self->retries > 0) {
				self->retries--;
				if(self->send_fn)
					self->send_fn(self->send_param);
				timeout_update(self->timeout);
				return 1;
			}
			// No retry remaining, peer was DEAD!!
			else
				return 0;
		}

		// Not timeout'd
		else
			return 1;
	}
	return 0;
}

// Reset retries
void
heartbeat_reset_retries(heartbeat_t *self)
{
	if(self) {
		self->retries = (int)(self->_deadtime / self->_keepalive);
		if((self->_deadtime % self->_keepalive) == 0)
			self->retries--;
	}
}

// This call is to reactivate timeout'd item
void
heartbeat_reactivate(heartbeat_t *self)
{
	if(self) {
		heartbeat_reset_retries(self);
		timeout_update(self->timeout);
	}
}

void
heartbeat_destroy(heartbeat_t **self_p)
{
	if(self_p && *self_p) {
		heartbeat_t *self = *self_p;
		timeout_destroy(&self->timeout);
		free(self);
		*self_p = NULL;
	}
}

timeout_t *
heartbeat_get_timeout(heartbeat_t *self)
{
	if(self)return self->timeout;
	else	return NULL;
}

uint64_t
heartbeat_get_deadtime(heartbeat_t *self)
{
	if(self)return self->_deadtime;
	else	return 0;
}

uint64_t
heartbeat_get_keepalive(heartbeat_t *self)
{
	if(self)return self->_keepalive;
	else	return 0;
}

int
heartbeat_get_retries(heartbeat_t *self)
{
	if(self)return self->retries;
	else	return 0;
}


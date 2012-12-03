#include "utils.h"

// [Delimiter ("LOVEMEPLZ")][Total Length (16bytes)][Data1][Data2]...[DataN]
// [DataN]: [Length (16bytes)][Data]

const char _SERIALIZE_DELIMITER[] = "LOVEMEPLZ";

#define DEFAULT_BUF_SIZE	512
struct _serialize_t {
	char		*_buf;
	uint64_t	_buf_size;
	uint64_t	_buf_pos;
};

serialize_t	*
serialize_create()
{
	serialize_t *self = (serialize_t*)zmalloc(sizeof(serialize_t));
	if(self) {
		self->_buf_pos = 0;
		self->_buf_size = DEFAULT_BUF_SIZE;
		self->_buf = (char *)zmalloc(sizeof(char) * self->_buf_size);
		if(!self->_buf) {
			free(self);
			self = NULL;
		}
	}
	return self;
}

// Reset the serialization buffer for reuse purpose.
void
serialize_reset(serialize_t *self)
{
	if(self) {
		self->_buf_pos = 0;
	}
}

void
serialize_destroy(serialize_t *self)
{
	if(self) {
		FREE(self->_buf);
		free(self);
	}
}

// Private function, to see if serialization buffer
// size is enough or not. If not, re-allocate the
// buffer.
//
// Return:
//   0 means success, 1 means failure occurs.
int
_adjust_serialize_buf(serialize_t *self, size_t needed)
{
	if(self) {
		needed += (self->_buf_pos + 1); // always prepare 1 byte for '\0'
		if(needed > self->_buf_size) {
			self->_buf_size = needed;
			self->_buf = (char *)realloc(self->_buf, sizeof(char) * self->_buf_size);
			if(!self->_buf) {
				return 1;
			}
		}
	}
	return 0;
}

// Private method to write uint64_t value into
// 16 bytes long character string. Private method
// doesn't need to call _adjust_serialize_buf().
void
_write_uint64(serialize_t *self, uint64_t length)
{
	if(self->_buf) {
		snprintf(self->_buf + self->_buf_pos, 17, "%016lx", length);
		self->_buf_pos += 16;
	}
}

// Write delimiter into serialization buffer.
//
// Return:
//   Return 0 means success, 1 means failed.
int
serialize_w_delimiter(serialize_t *self)
{
	if(self->_buf) {
		int size = strlen(_SERIALIZE_DELIMITER);
		if(_adjust_serialize_buf(self, size) == 0) {
			memcpy(self->_buf + self->_buf_pos, _SERIALIZE_DELIMITER, size);
			self->_buf_pos += size;
			return 0;
		}
	}
	return 1;
}

// Write uint64_t value into serialization buffer.
//
// Return:
//   Return 0 means success, 1 means failed.
int
serialize_w_uint64(serialize_t *self, uint64_t src)
{
	if(self) {
		if(_adjust_serialize_buf(self, 32) == 0) {
			_write_uint64(self, 16);

			_write_uint64(self, src);

			return 0;
		}
	}
	return 1;
}

// Write string into serialization buffer.
//
// Return:
//   Return 0 means success, 1 means failed.
int
serialize_w_str(serialize_t *self, char *src)
{
	if(self && src) {
		int size = strlen(src);
		if(_adjust_serialize_buf(self, 16 + size) == 0) { // +16 for length
			// Write the length of string, takes 16 bytes
			_write_uint64(self, size);

			// Copy string
			memcpy(self->_buf + self->_buf_pos, src, size);
			self->_buf_pos += size;

			return 0;
		}
	}
	return 1;
}

// Add DELIMITER and totoal length of serialization buffer to the
// beginning of it. Please remember to free when no more use.
//
// Return:
//   Return 0 means success, 1 means failed.
//char *
int
serialize_w_done(serialize_t *self)
{
	if(self) {
		int hdr_size = strlen(_SERIALIZE_DELIMITER) + 16;
		if(_adjust_serialize_buf(self, hdr_size) == 0) {
			char *hdr = (char *)zmalloc(sizeof(char) * hdr_size);
			if(hdr) {
				memmove(self->_buf + hdr_size, self->_buf, self->_buf_pos);
				snprintf(self->_buf, hdr_size + 1, "%s%016lx", _SERIALIZE_DELIMITER, self->_buf_pos);
				self->_buf_pos += hdr_size;
				self->_buf[self->_buf_pos] = '\0';
				return 0;
			}
		}
	}
	return 1;
}

int
serialize_w_serialize(serialize_t *dst, serialize_t *src)
{
	if(src && dst) {
		if(_adjust_serialize_buf(dst, src->_buf_pos) == 0) {
			memcpy(dst->_buf + dst->_buf_pos, src->_buf, src->_buf_pos);
			dst->_buf_pos += src->_buf_pos;
			return 0;
		}
	}
	return 1;
}

char *
serialize_bufdup(serialize_t *self)
{
	char *ret = NULL;
	if(self) {
		ret = (char *)zmalloc(sizeof(char) * (self->_buf_pos + 1));
		if(ret) {
			memcpy(ret, self->_buf, self->_buf_pos);
			ret[self->_buf_pos] = '\0';
		}
	}
	return ret;
}

int
serialize_bufset(serialize_t *self, char *buf)
{
	if(self && buf) {
		int buf_len = (int)strlen(buf);
		if(_adjust_serialize_buf(self, buf_len) == 0) {
			memcpy(self->_buf + self->_buf_pos, buf, buf_len);
			self->_buf[buf_len] = '\0';
			return 0;
		}
	}
	return 1;
}

uint64_t
_read_uint64(char *tmp)
//, serialize_t *self)
{
	uint64_t c = 0;
	if(tmp) {
	//if(self)
		//char *tmp = self->_buf + self->_buf_pos;
		uint64_t value;
		int tmp_len = (strlen(tmp) >= 16) ? 16 : strlen(tmp);
		for(int i = 0; i < tmp_len; i++, tmp++) {
			// Number
			if     ((*tmp >= '0') && (*tmp <= '9'))	value = (uint64_t)(*tmp - '0');
			// a~f
			else if((*tmp >= 'a') && (*tmp <= 'f'))	value = (uint64_t)(*tmp - 'a' + 10);
			// A~F
			else if((*tmp >= 'A') && (*tmp <= 'F'))	value = (uint64_t)(*tmp - 'A' + 10);
			// Unknown
			else continue;
			c += (value << ((tmp_len - 1 - i) << 2));
		}
		//self->_buf_pos += 16;
	}
	return c;
}

// Prepare a serialization buffer by passing a char array.
// This function searches for _SERIALIZE_DELIMITER and read
// the serialization buffer length after the delimiter.
//
// Return:
//   Return serialize_t * when success, or NULL when failed.
int
serialize_r_prepare(//char *src,
		serialize_t *buf)
{
	if(//src && buf) {
			buf) {
		// Find delimiter
//		char *tmp = strstr(src, _SERIALIZE_DELIMITER);
		char *tmp = strstr(buf->_buf + buf->_buf_pos, _SERIALIZE_DELIMITER);
		if(tmp) {
			// make sure src length is enough to get serialization buf length (16 bytes long)
			tmp += strlen(_SERIALIZE_DELIMITER);
			if(strlen(tmp) > 16) {
				// Get serialization buf length
//				uint64_t size = _read_uint64(tmp);
				// make sure src length is enough to get serialization buf data
				tmp += 16;
//				if(strlen(tmp) >= size) {
//					buf->_buf_pos = 0;
//					buf->_buf_size = size + 1;
//					buf->_buf = (char *)zmalloc(sizeof(char) * buf->_buf_size);
//					if(buf->_buf) {
//						memcpy(buf->_buf, tmp, size);
//						buf->_buf[size] = '\0';
//						return 0;
//					}
//				}
				buf->_buf_pos = tmp - buf->_buf;
				return 0;
			}
		}
	}
	return 1;
}

// Read uint64_t value from serialization buffer.
//
// Return:
//   Return uint64_t value.
uint64_t
serialize_r_uint64(serialize_t *self)
{
	uint64_t c = 0;
	if(self) {
		uint64_t value = _read_uint64(self->_buf + self->_buf_pos);
		self->_buf_pos += 16;
		if(value == 16)
			c = _read_uint64(self->_buf + self->_buf_pos);
		self->_buf_pos += 16;
	}
	return c;
}

// Read string from serialization buffer, please
// remember to free when no more use.
//
// Return:
//   Return the pointer of char array.
char *
serialize_r_str(serialize_t *self)
{
	char *ret = NULL;
	if(self) {
		// Get string length
		uint64_t size = _read_uint64(self->_buf + self->_buf_pos);
		self->_buf_pos += 16;

		// Get string
		if(size > 0) {
			ret = (char *)zmalloc(sizeof(char) * (size + 1));
			if(ret) {
				memcpy(ret, self->_buf + self->_buf_pos, size);
				ret[size] = '\0';
			}
			self->_buf_pos += size;
		}
	}
	return ret;
}


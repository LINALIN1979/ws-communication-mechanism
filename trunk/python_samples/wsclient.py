# Import Python ctypes support
DISPATCHER="tcp://172.17.153.190:5555"

# Load C shared library
from ctypes import *  # in order to omit typing "ctypes." before any ctypes related usage 
#client = cdll.LoadLibrary("libwsclient.so")
#c = cast(client.client_create(DISPATCHER), c_void_p)

import ctypes
client = ctypes.CDLL("libwsclient.so")
client.client_create.argtypes = [c_char_p]
client.client_create.restype = c_void_p
client.client_oplong.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p]
client.client_oplong.restype = c_char_p
client.client_querytask.argtypes = [c_void_p, c_char_p]
client.client_querytask.restype = c_uint
client.client_opshort.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p]
client.client_opshort.restype = c_char_p
client.client_destroy.argtypes = [POINTER(c_void_p)]
c = client.client_create(DISPATCHER)


print "oplong..."
#taskid = cast(client.client_oplong(c, "time.1", "method:time", "what time is it?"), c_char_p)
taskid = client.client_oplong(c, "time.1", "method:time", "what time is it?")
print taskid
quit = 0
percentage = 0
while not quit:
	import time
	time.sleep(2)
	percentage = client.client_querytask(c, taskid)
	if percentage == 100:
		print "Task done!"
		quit = 1
	elif percentage == 904:
		print "Task failed!"
		quit = 1
	elif percentage == 402:
		print "Task not found!"
		quit = 1
	else:
		print percentage 	

print "opshort..."
#data = cast(client.client_opshort(c, "iscsi.boss", "method:time", "what time is it?"), c_char_p)
#print data.value
data = client.client_opshort(c, "time.1", "method:time", "what time is it?")
if data == None:
	print "Null string returned"
else:
	print data

#data = cast(client.client_opshort(c, "iscsi", "method:time", "what time is it?"), c_char_p)
#print data.value
data = client.client_opshort(c, "time", "method:time", "what time is it?")
if data == None:
	print "Null string returned"
else:
	print data

#client.client_destroy(byref(c))
client.client_destroy(byref(cast(c, c_void_p)))
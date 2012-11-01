# Import Python ctypes support
DISPATCHER="tcp://localhost:5555"

from ctypes import *

# Load C shared library
client = cdll.LoadLibrary("libwsclient.so")

c = cast(client.client_create(DISPATCHER), c_void_p)

print "oplong..."
taskid = cast(client.client_oplong(c, "time.1", "method:time", "what time is it?"), c_char_p)
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
data = cast(client.client_opshort(c, "iscsi.boss", "method:time", "what time is it?"), c_char_p)
print data.value

data = cast(client.client_opshort(c, "iscsi", "method:time", "what time is it?"), c_char_p)
print data.value

client.client_destroy(byref(c))

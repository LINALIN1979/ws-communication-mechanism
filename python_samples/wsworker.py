DISPATCHER="tcp://localhost:5555"
HOSTNAME="iscsi.boss"


import time
from ctypes import * # Import Python ctypes support

# Load C shared library
worker = cdll.LoadLibrary("libwsworker.so")

# Callback function declarations
WORKER_OPLONG_FUNC = CFUNCTYPE(c_int, c_char_p, c_char_p, c_char_p)
WORKER_OPSHORT_FUNC = CFUNCTYPE(c_int, c_char_p, c_char_p)

# Callback function definition
got_oplong_job = 0
taskid = ""
def worker_oplong_fn(_taskid, _method, _param):
	print "Get oplong:"
	print "  task ID: %s" % _taskid
	print "  method: %s" % _method
	print "  param: %s" % _param
	global got_oplong_job
	global taskid
	got_oplong_job = 1
	taskid = _taskid
	return 0

got_opshort_job = 0
opshort_ret = create_string_buffer(100) # must be global, otherwise will be free by Python
def worker_opshort_fn(_method, _param):
	print "Get opshort:"
	print "  method: %s" % _method
	print "  param: %s" % _param
	global got_opshort_job
	global opshort_ret
	got_opshort_job = 1
	from time import gmtime, strftime
	opshort_ret.value = strftime("%a, %d %b %Y %H:%M:%S", gmtime())
	return addressof(opshort_ret)

# Assign callback functions
oplong = WORKER_OPLONG_FUNC(worker_oplong_fn)
opshort = WORKER_OPSHORT_FUNC(worker_opshort_fn)


w = cast(worker.worker_create(HOSTNAME, DISPATCHER, oplong, opshort), c_void_p)

# Wait for oplong_job
while not got_oplong_job:
	time.sleep(2)
for x in range(1, 11):
	worker.worker_update(w, taskid, x * 10)
	time.sleep(0.3)

# Wait for opshort_job
while not got_opshort_job:
	time.sleep(2)

worker.worker_destroy(byref(w))


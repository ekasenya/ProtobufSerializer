#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include "deviceapps.pb-c.h"

#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

typedef struct pbheader_s {
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;
#define PBHEADER_INIT {MAGIC, 0, 0}

const char *DEVICE_KEY = "device";
const char *LAT_KEY = "lat";
const char *LON_KEY = "lon";
const char *APPS_KEY = "apps";

const char *TYPE_KEY = "type";
const char *ID_KEY = "id";

// https://github.com/protobuf-c/protobuf-c/wiki/Examples
void example() {
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    void *buf;
    unsigned len;

    char *device_id = "e7e1a50c0ec2747ca56cd9e1558c0d7c";
    char *device_type = "idfa";
    device.has_id = 1;
    device.id.data = (uint8_t*)device_id;
    device.id.len = strlen(device_id);
    device.has_type = 1;
    device.type.data = (uint8_t*)device_type;
    device.type.len = strlen(device_type);
    msg.device = &device;

    msg.has_lat = 1;
    msg.lat = 67.7835424444;
    msg.has_lon = 1;
    msg.lon = -22.8044005471;

    msg.n_apps = 3;
    msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);

    msg.apps[0] = 42;
    msg.apps[1] = 43;
    msg.apps[2] = 44;
    len = device_apps__get_packed_size(&msg);

    buf = malloc(len);
    device_apps__pack(&msg, buf);

    fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
    fwrite(buf, len, 1, stdout); // Write to stdout to allow direct command line piping

    free(msg.apps);
    free(buf);
}


// Parse deviceapps item of input list
// Return DeviceApps msg
static int parse_item(PyObject *dict_item, DeviceApps *msg) {
    //DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    msg->device = &device;

    PyObject *device_info = PyDict_GetItemString(dict_item, DEVICE_KEY);
    if ( PyDict_Check(device_info) ) {
        PyObject *type = PyDict_GetItemString(device_info, TYPE_KEY);
        if ( PyMapping_Check(type) ) {
            char *device_type = PyUnicode_AsUTF8(type);
            device.has_type = 1;
            device.type.data = (uint8_t*)device_type;
            device.type.len = strlen(device_type);
        }

        PyObject *id = PyDict_GetItemString(device_info, ID_KEY);
        if ( PyMapping_Check(id) ) {
            char *device_id = PyUnicode_AsUTF8(id);
            device.has_id = 1;
            device.id.data = (uint8_t*)device_id;
            device.id.len = strlen(device_id);
        }
    }

    PyObject *lat = PyDict_GetItemString(dict_item, LAT_KEY);
    if ( ( PyFloat_Check(lat) || PyLong_Check(lat) ) ) {
        msg->has_lat = 1;
        msg->lat = PyFloat_AsDouble(lat);
    }

    PyObject *lon = PyDict_GetItemString(dict_item, LON_KEY);
    if ( ( PyFloat_Check(lon) || PyLong_Check(lon) ) ) {
        msg->has_lon = 1;
        msg->lon = PyFloat_AsDouble(lon);
    }

    PyObject *apps = PyDict_GetItemString(dict_item, APPS_KEY);
    if ( PyList_Check(apps) ) {
        int size = PyList_Size(apps);

        msg->n_apps = size;
        msg->apps = malloc(sizeof(uint32_t) * msg->n_apps);

        if (msg->apps == NULL) {
            return 1;
        }

        int i = 0;
        for (i = 0; i < size; i++)
        {
            PyObject *app = PyList_GetItem(apps, i);
            if ( PyLong_Check(app) ) {
                msg->apps[i] = PyLong_AsUnsignedLong(app);
            } else {
                msg->apps[i] = 0;
            }
        }
    }

    return 0;
}


// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args) {
    const char* path;
    PyObject* list;

    if (!PyArg_ParseTuple(args, "Os", &list, &path))
        return NULL;

    if ( !PyList_Check(list) ) {
        PyErr_SetString(PyExc_TypeError, "Input parameter is not a list");
        return NULL;
    }

    int bytes_written = 0;

    printf("********************************************\n");
    printf("Write to: %s\n", path);

    gzFile fp = gzopen(path,"w");
    if (!fp) {
        PyErr_Format(PyExc_OSError, "Can not create or open file1 %s", path);
        return NULL;
    }

    int size = PyList_Size(list);

    int i = 0;
    void *buf;
    unsigned len;

    for (i = 0; i < size; i++)
    {
        PyObject *dict = PyList_GetItem(list, i);
        if ( PyDict_Check(dict) ) {
            DeviceApps msg = DEVICE_APPS__INIT;
            int res = parse_item(dict, &msg);
            if (res == 1) {
                PyErr_SetString(PyExc_MemoryError, "Can not allocate memory");
                return NULL;
            }

            len = device_apps__get_packed_size(&msg);

            buf = malloc(len);

            if (buf == NULL) {
                gzclose(fp);
                free(msg.apps);

                PyErr_SetString(PyExc_MemoryError, "Can not allocate memory");
                return NULL;
            }

            device_apps__pack(&msg, buf);

            pbheader_t header = PBHEADER_INIT;
            header.magic = MAGIC;
            header.type = DEVICE_APPS_TYPE;
            header.length = len;

            gzwrite(fp, &header, sizeof(header));

            fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
            gzwrite(fp, buf, len); // Write to file

            bytes_written += len;
            free(msg.apps);
            free(buf);
        }
    }

    gzclose(fp);

    printf("********************************************\n");

    return PyLong_FromLong(bytes_written);
}

typedef struct {
    PyObject_HEAD
    Py_ssize_t seq_index, enum_index;
    PyObject *sequence;
} DeviceAppGenState;

static PyObject* devappgen_new(PyTypeObject *type, PyObject *args, PyObject *kwargs);
static void devappgen_dealloc(DeviceAppGenState *dastate);
static PyObject* devappgen_next(DeviceAppGenState *dastate);

static PyTypeObject PyDeviceAppGen_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "devappgen",                    /* tp_name */
    sizeof(DeviceAppGenState),      /* tp_basicsize */
    0,                              /* tp_itemsize */
    (destructor)devappgen_dealloc,  /* tp_dealloc */
    0,                              /* tp_print */
    0,                              /* tp_getattr */
    0,                              /* tp_setattr */
    0,                              /* tp_reserved */
    0,                              /* tp_repr */
    0,                              /* tp_as_number */
    0,                              /* tp_as_sequence */
    0,                              /* tp_as_mapping */
    0,                              /* tp_hash */
    0,                              /* tp_call */
    0,                              /* tp_str */
    0,                              /* tp_getattro */
    0,                              /* tp_setattro */
    0,                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,             /* tp_flags */
    0,                              /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    PyObject_SelfIter,              /* tp_iter */
    (iternextfunc)devappgen_next,   /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
    PyType_GenericAlloc,            /* tp_alloc */
    devappgen_new,                  /* tp_new */
};


static PyObject* devappgen_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    PyObject *sequence;

    if (!PyArg_UnpackTuple(args, "devappgen", 1, 1, &sequence))
        return NULL;

    /* We expect an argument that supports the sequence protocol */
    if (!PySequence_Check(sequence)) {
        PyErr_SetString(PyExc_TypeError, "devappgen() expects a sequence");
        return NULL;
    }

    Py_ssize_t len = PySequence_Length(sequence);
    if (len == -1)
        return NULL;

    /* Create a new RevgenState and initialize its state - pointing to the last
     * index in the sequence.
    */
    DeviceAppGenState *dastate = (DeviceAppGenState *)type->tp_alloc(type, 0);
    if (!dastate)
        return NULL;

    Py_INCREF(sequence);
    dastate->sequence = sequence;
    dastate->seq_index = len - 1;
    dastate->enum_index = 0;

    return (PyObject *)dastate;
}

static void devappgen_dealloc(DeviceAppGenState *dastate)
{
    /* We need XDECREF here because when the generator is exhausted,
     * rgstate->sequence is cleared with Py_CLEAR which sets it to NULL.
    */
    Py_XDECREF(dastate->sequence);
    Py_TYPE(dastate)->tp_free(dastate);
}

static PyObject* devappgen_next(DeviceAppGenState *dastate)
{
    /* seq_index < 0 means that the generator is exhausted.
     * Returning NULL in this case is enough. The next() builtin will raise the
     * StopIteration error for us.
    */
    if (dastate->seq_index >= 0) {
        PyObject *elem = PySequence_GetItem(dastate->sequence,
                                            dastate->seq_index);
        /* Exceptions from PySequence_GetItem are propagated to the caller
         * (elem will be NULL so we also return NULL).
        */
        if (elem) {
            PyObject *result = Py_BuildValue("lO", dastate->enum_index, elem);
            dastate->seq_index--;
            dastate->enum_index++;
            return result;
        }
    }

    /* The reference to the sequence is cleared in the first generator call
     * after its exhaustion (after the call that returned the last element).
     * Py_CLEAR will be harmless for subsequent calls since it's idempotent
     * on NULL.
    */
    dastate->seq_index = -1;
    Py_CLEAR(dastate->sequence);
    return NULL;
}

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args) {
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    printf("Read from: %s\n", path);
    Py_RETURN_NONE;
}


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};

static struct PyModuleDef pbmodule = {
    PyModuleDef_HEAD_INIT,
    "pb",
    "Protobuf serializer",
    -1,
    PBMethods
};

PyMODINIT_FUNC PyInit_pb(void) {
    PyObject *m = PyModule_Create(&pbmodule);

    PyDeviceAppGen_Type.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PyDeviceAppGen_Type) < 0)  return;
    //Py_INCREF(&PyDeviceAppGen_Type);
    //PyModule_AddObject(m, "iter", (PyObject *)&PyDeviceAppGen_Type);
}

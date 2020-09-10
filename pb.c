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
static DeviceApps parse_item(PyObject *dict_item) {
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    msg.device = &device;

    PyObject *device_info = PyDict_GetItemString(dict_item, DEVICE_KEY);
    if ( (device_info != NULL) && PyDict_Check(device_info) ) {
        PyObject *type = PyDict_GetItemString(device_info, TYPE_KEY);
        if ( (type != NULL) && PyMapping_Check(type) ) {
            char *device_type = PyUnicode_AsUTF8(type);
            device.has_type = 1;
            device.type.data = (uint8_t*)device_type;
            device.type.len = strlen(device_type);
        }

        PyObject *id = PyDict_GetItemString(device_info, ID_KEY);
        if ( (id != NULL) && PyMapping_Check(id)) {
            char *device_id = PyUnicode_AsUTF8(id);
            device.has_id = 1;
            device.id.data = (uint8_t*)device_id;
            device.id.len = strlen(device_id);
        }
    }

    PyObject *lat = PyDict_GetItemString(dict_item, LAT_KEY);
    if ( (lat != NULL) && ( PyFloat_Check(lat) || PyLong_Check(lat) ) ) {
        msg.has_lat = 1;
        msg.lat = PyFloat_AsDouble(lat);
    }

    PyObject *lon = PyDict_GetItemString(dict_item, LON_KEY);
    if ( (lon != NULL) && ( PyFloat_Check(lon) || PyLong_Check(lon) ) ) {
        msg.has_lon = 1;
        msg.lon = PyFloat_AsDouble(lon);
    }

    PyObject *apps = PyDict_GetItemString(dict_item, APPS_KEY);
    if ( PyList_Check(apps) ) {
        int size = PyList_Size(apps);

        msg.n_apps = size;
        msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);

        int i = 0;
        for (i = 0; i < size; i++)
        {
            PyObject *app = PyList_GetItem(apps, i);
            if ( PyLong_Check(app) ) {
                msg.apps[i] = PyLong_AsUnsignedLong(app);
            } else {
                msg.apps[i] = 0;
            }
        }
    }

    return msg;
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
        printf("Input parameter is not a list\n");
        return NULL;
    }

    int bytes_written = 0;

    printf("********************************************\n");
    printf("Write to: %s\n", path);
    gzFile fp = gzopen(path,"a+");

    int size = PyList_Size(list);
    printf("size=%d\n", size);

    int i = 0;
    void *buf;
    unsigned len;

    for (i = 0; i < size; i++)
    {
        PyObject *dict = PyList_GetItem(list, i);
        if ( PyDict_Check(dict) ) {
            DeviceApps msg = parse_item(dict);
            printf("type=%s, id=%s, lat=%.10f, lon=%.10f\n", msg.device->type.data, msg.device->id.data, msg.lat, msg.lon);

            len = device_apps__get_packed_size(&msg);

            buf = malloc(len);
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
     (void) PyModule_Create(&pbmodule);
}

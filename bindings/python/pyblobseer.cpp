/**
 * Copyright (C) 2009 Benjamin Giraud <benjamin.girault@eleves.bretagne.ens-cachan.fr>
 *
 * This file is part of BlobSeer, a scalable distributed big data
 * store. Please see README (root of repository) for more details.
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses>.
 */

#include <boost/python.hpp>
#include <boost/python/args.hpp>
#include <boost/bind.hpp>
#include <boost/mpl/vector.hpp>
#include "client/object_handler.hpp"

#include <iostream>
using namespace boost::python;

class py_object_handler : public object_handler
{
public:
    py_object_handler(const std::string &config_file) : object_handler(config_file) {}

    /* New function to read from blobseer so that the binding handles python StringIO buffers */
    bool py_read(boost::uint64_t offset, boost::uint64_t size, PyObject* py_buffer, boost::uint32_t version = 0) {
        PyObject* buffer_class = PyObject_Str(PyObject_GetAttr(py_buffer, PyString_FromString("__class__")));
        PyObject* stringio_str = PyString_FromString("StringIO");
        long find = PyInt_AsLong(PyObject_CallMethodObjArgs(buffer_class, PyString_FromString("find"), stringio_str, NULL));
        if (find == -1) {
            PyObject_Print(PyString_FromString("3rd argument of pyblobseer.object_handler.read must be a StringIO!\n"),stderr,Py_PRINT_RAW);
            return false;
        }
        char* buffer = new char[size];
        if (this->object_handler::read(offset,size,buffer,version)) {
            PyObject_CallMethodObjArgs(py_buffer, PyString_FromString("write"), PyString_FromString(buffer),NULL);
            return true;
        } else
            return false;
    }

    /* To be able to have a property size in the python module (we need a "get" function, with no argument, for that) */
    boost::uint64_t size() {
        return get_size();
    }
};

BOOST_PYTHON_MODULE(pyblobseer)
{
    class_<py_object_handler, boost::noncopyable>("object_handler", init<std::string>())
        .def("create", &py_object_handler::create, (arg("page_size"),arg("replica_count") = 1))
        .def("get_latest", &py_object_handler::get_latest, (arg("id") = 0))
        .def("read", &py_object_handler::py_read, (arg("offset"), arg("size"), arg("buffer"), arg("version") = 0))
        .def("write", &py_object_handler::write, (arg("offset"), arg("size"), arg("buffer")))
        .def("append", &py_object_handler::append, (arg("size"),arg("buffer")))
        .def("get_size", &py_object_handler::get_size, (arg("version") = 0))
        .add_property("size", &py_object_handler::size)
        .add_property("objcount", &py_object_handler::get_objcount)
        .add_property("version", &py_object_handler::get_version)
        .add_property("page_size", &py_object_handler::get_page_size)
        .add_property("id", &py_object_handler::get_id);
}

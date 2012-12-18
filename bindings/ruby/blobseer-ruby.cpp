/**
 * Copyright (C) 2009 Mathieu Dorier <matthieu.dorier@inria.fr>
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


// Include the blobseer headers
#include "client/object_handler.hpp"
// Include the Ruby headers
#include "ruby.h"

// because Ruby can't wrap a C++ class into a Ruby class, we have to
// create a structure with a void* pointer inside to do it.
typedef struct cpp_obj_wrapper_ {
	object_handler* data;
} *cpp_obj_wrapper;

// Defining a space for information and references about the class to be stored internally
VALUE c_CppObjectHandler = Qnil;
VALUE m_BlobSeer = Qnil;

// free a wrapped c++ object
static void cpp_obj_wrapper_free(cpp_obj_wrapper obj) 
{
	if(obj->data != NULL)
		delete obj->data;
	free(obj);
}

// create a c++ object to be parsed in ruby
static cpp_obj_wrapper create_cpp_obj_wrapper(object_handler* data)
{
	cpp_obj_wrapper obj = (cpp_obj_wrapper)malloc(sizeof(struct cpp_obj_wrapper_));
	obj->data = data;
	return obj;
}

// Function for initialisation of blobseer
VALUE method_init_object_handler(VALUE self, VALUE rb_str_config)
{
	try {
		std::string c_str_config(RSTRING(rb_str_config)->ptr);
		cpp_obj_wrapper c_object_handler = create_cpp_obj_wrapper(new object_handler(c_str_config));
		VALUE rb_object_handler = Data_Wrap_Struct(c_CppObjectHandler, NULL, cpp_obj_wrapper_free, c_object_handler);
		return rb_object_handler;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
}	

// Creates a new blob
VALUE method_create(VALUE self, VALUE rb_object_handler, VALUE rb_page_size, VALUE rb_replica_count)
{
	try {
		uint64_t c_page_size = (uint64_t)NUM2LL(rb_page_size);
		unsigned int c_replica_count = NUM2UINT(rb_replica_count);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		if(((object_handler*)(c_object_handler->data))->create(c_page_size,c_replica_count)) return Qtrue;
		else return Qfalse;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qfalse;
	}
}

// Clone a blob into a new one
VALUE method_clone(VALUE self, VALUE rb_object_handler, VALUE rb_id, VALUE rb_version)
{
	try {
		int32_t c_id = NUM2INT(rb_id);
		int32_t c_version = NUM2INT(rb_version);
		printf("cloning version %d of blob %d\n",c_version,c_id);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		if(((object_handler*)(c_object_handler->data))->clone(c_id,c_version)) return Qtrue;
		else return Qfalse;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qfalse;
	}
}

// Get the latest version of a blob
VALUE method_get_latest(VALUE self, VALUE rb_object_handler, VALUE rb_id)
 {
	try {
		uint32_t c_id = NUM2UINT(rb_id);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		if(((object_handler*)(c_object_handler->data))->get_latest(c_id)) return Qtrue;
		else return Qfalse;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qfalse;
	}
 }
 
 
// Get locations of chunks
VALUE method_get_locations(VALUE self, VALUE rb_object_handler, VALUE rb_offset, VALUE rb_size, VALUE rb_version)
{
	try {
		object_handler::page_locations_t loc;
		uint64_t c_offset = (uint64_t)NUM2LL(rb_offset);
		uint64_t c_size = (uint64_t)NUM2LL(rb_size);
		uint32_t c_version = (uint32_t)NUM2UINT(rb_version);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_, c_object_handler);
		
		if (!(object_handler*)(c_object_handler->data)->get_locations(loc, c_offset, c_size, c_version))
			return Qnil;

		VALUE rb_arr_result = rb_ary_new();
		for(unsigned i = 0; i < loc.size(); i++) {
			VALUE plocation = rb_ary_new();

			VALUE rb_host = rb_str_new(loc[i].provider.host.c_str(),strlen(loc[i].provider.host.c_str()));
			VALUE rb_port = rb_str_new(loc[i].provider.service.c_str(),strlen(loc[i].provider.service.c_str()));
			VALUE loc_offset = ULL2NUM(loc[i].offset);
			VALUE loc_size = ULL2NUM(loc[i].size);
			
			rb_ary_push(plocation,rb_host);
			rb_ary_push(plocation,rb_port);
			rb_ary_push(plocation,loc_offset);
			rb_ary_push(plocation,loc_size);
			
			rb_ary_push(rb_arr_result, plocation);
		}
		return rb_arr_result;

	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
}
// Read in a blob
 VALUE method_read(VALUE self, VALUE rb_object_handler, VALUE rb_offset, VALUE rb_size, VALUE rb_version)
 {
	try {
		uint64_t c_offset = (uint64_t)NUM2LL(rb_offset);
		uint64_t c_size = (uint64_t)NUM2LL(rb_size);
		uint32_t c_version = (uint32_t)NUM2UINT(rb_version);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);

		char fake_str[1]; fake_str[0] = '\0';
		VALUE rb_str = rb_str_new(fake_str,0);
		RSTRING(rb_str)->len = c_size;
		char* c_buffer = (char*)calloc(c_size,sizeof(char));
		RSTRING(rb_str)->ptr = c_buffer;

		if(((object_handler*)(c_object_handler->data))->read(c_offset, c_size, c_buffer, c_version))
		{
			return rb_str;
		}
		else
		{	
			free(c_buffer);
			return Qnil;
		}
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }
 
 // Append data in a blob
 VALUE method_append(VALUE self, VALUE rb_object_handler, VALUE rb_size, VALUE rb_buffer)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint64_t c_size = NUM2LL(rb_size);
		if(((object_handler*)(c_object_handler->data))->append(c_size,RSTRING(rb_buffer)->ptr))
			return Qtrue;
		else
			return Qfalse;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qfalse;
	}
 }
 
 // Write in a blob
 VALUE method_write(VALUE self, VALUE rb_object_handler, VALUE rb_offset, VALUE rb_size, VALUE rb_buffer)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint64_t c_offset = (uint64_t)NUM2LL(rb_offset);
		uint64_t c_size = (uint64_t)NUM2LL(rb_size);
		if(((object_handler*)(c_object_handler->data))->write(c_offset, c_size, RSTRING(rb_buffer)->ptr))
			return Qtrue;
		else
			return Qfalse;
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qfalse;
	}
 }
 
 // Return the number of blobs
 VALUE method_get_objcount(VALUE self, VALUE rb_object_handler)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		int c_objcount = ((object_handler*)(c_object_handler->data))->get_objcount();
		return INT2NUM(c_objcount);
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }
 
 // Get the size of a blob
 VALUE method_get_size(VALUE self, VALUE rb_object_handler, VALUE rb_version)
 {
	try {
		uint32_t c_version = NUM2INT(rb_version);
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint64_t c_size =  ((object_handler*)(c_object_handler->data))->get_size(c_version);
		return UINT2NUM(c_size);
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }
 
 // Get the version of a blob
 VALUE method_get_version(VALUE self, VALUE rb_object_handler)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint64_t c_version =  ((object_handler*)(c_object_handler->data))->get_version();
		return UINT2NUM(c_version);
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }
 
 // Get the page size of a blob
 VALUE method_get_page_size(VALUE self, VALUE rb_object_handler)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint64_t c_page_size =  ((object_handler*)(c_object_handler->data))->get_page_size();
		return UINT2NUM(c_page_size);
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }
 
 // Get the current id
 VALUE method_get_id(VALUE self, VALUE rb_object_handler)
 {
	try {
		cpp_obj_wrapper c_object_handler;
		Data_Get_Struct(rb_object_handler, struct cpp_obj_wrapper_ , c_object_handler);
		uint32_t c_id = ((object_handler*)(c_object_handler->data))->get_id();
		return UINT2NUM(c_id);
	} catch (std::runtime_error &e) {
		printf("%s\n",e.what());
		return Qnil;
	}
 }

// Initialize the Ruby library with the previous module functions
extern "C" void Init_libblobseer_ruby()
{
	m_BlobSeer = rb_define_module("BLOBSEER");
	c_CppObjectHandler = rb_define_class_under(m_BlobSeer, "CppObjectHandler", rb_cObject);
	rb_define_module_function(m_BlobSeer, "_create_", reinterpret_cast<VALUE (*)(...)>(method_create), 3);
	rb_define_module_function(m_BlobSeer, "_clone_", reinterpret_cast<VALUE (*)(...)>(method_clone), 3);
	rb_define_module_function(m_BlobSeer, "_read_", reinterpret_cast<VALUE (*)(...)>(method_read), 4);
	rb_define_module_function(m_BlobSeer, "_write_", reinterpret_cast<VALUE (*)(...)>(method_write), 4);
	rb_define_module_function(m_BlobSeer, "_get_id_", reinterpret_cast<VALUE (*)(...)>(method_get_id), 1);
	rb_define_module_function(m_BlobSeer, "_get_size_", reinterpret_cast<VALUE (*)(...)>(method_get_size), 2);
	rb_define_module_function(m_BlobSeer, "_get_locations_", reinterpret_cast<VALUE (*)(...)>(method_get_locations),4);
	rb_define_module_function(m_BlobSeer, "_get_latest_", reinterpret_cast<VALUE (*)(...)>(method_get_latest), 2);
	rb_define_module_function(m_BlobSeer, "_get_page_size_",reinterpret_cast<VALUE (*)(...)>(method_get_page_size), 1);
	rb_define_module_function(m_BlobSeer, "_append_", reinterpret_cast<VALUE (*)(...)>(method_append), 3);
	rb_define_module_function(m_BlobSeer, "_get_version_", reinterpret_cast<VALUE (*)(...)>(method_get_version), 1);
	rb_define_singleton_method(c_CppObjectHandler,"new",reinterpret_cast<VALUE (*)(...)>(method_init_object_handler),1);
}

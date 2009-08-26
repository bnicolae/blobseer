// Include the blobseer headers
#include "client/object_handler.hpp"
// Include the Ruby headers
#include "ruby.h"

// because Ruby can't wrap a C++ class into a Ruby class, we have to
// create a structure with a void* pointer inside to do it.
typedef struct cpp_obj_wrapper_ {
	void *data;
} *cpp_obj_wrapper;

// Defining a space for information and references about the class to be stored internally
VALUE c_CppObjectHandler = Qnil;
VALUE m_BlobSeer = Qnil;

// free a wrapped c++ object
static void cpp_obj_wrapper_free(cpp_obj_wrapper obj) 
{
	free(obj);
}

// create a c++ object to be parsed in ruby
static cpp_obj_wrapper create_cpp_obj_wrapper(void* data)
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
		return Qfalse;
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
		char* c_buffer = (char*)calloc(c_size,sizeof(char));
		if(((object_handler*)(c_object_handler->data))->read(c_offset, c_size, c_buffer, c_version))
		{
			VALUE rb_str = rb_str_new(c_buffer,c_size);
			free(c_buffer);
			return rb_str;
		}
		else
		{
			return Qnil;
		}
	} catch (std::runtime_error &e) {
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
		if(((object_handler*)(c_object_handler->data))->append(c_size,STR2CSTR(rb_buffer)))
			return Qtrue;
		else
			return Qfalse;
	} catch (std::runtime_error &e) {
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
		if(((object_handler*)(c_object_handler->data))->write(c_offset, c_size, STR2CSTR(rb_buffer)))
			return Qtrue;
		else
			return Qfalse;
	} catch (std::runtime_error &e) {
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
		return Qnil;
	}
 }

// Initialize the Ruby library with the previous module functions
extern "C" void Init_libblobseer_ruby()
{
	m_BlobSeer = rb_define_module("BLOBSEER");
	c_CppObjectHandler = rb_define_class_under(m_BlobSeer, "CppObjectHandler", rb_cObject);
	rb_define_module_function(m_BlobSeer, "_create_", reinterpret_cast<VALUE (*)(...)>(method_create), 3);
	rb_define_module_function(m_BlobSeer, "_read_", reinterpret_cast<VALUE (*)(...)>(method_read), 4);
	rb_define_module_function(m_BlobSeer, "_write_", reinterpret_cast<VALUE (*)(...)>(method_write), 4);
	rb_define_module_function(m_BlobSeer, "_get_id_", reinterpret_cast<VALUE (*)(...)>(method_get_id), 1);
	rb_define_module_function(m_BlobSeer, "_get_size_", reinterpret_cast<VALUE (*)(...)>(method_get_size), 2);
	rb_define_module_function(m_BlobSeer, "_get_latest_", reinterpret_cast<VALUE (*)(...)>(method_get_latest), 2);
	rb_define_module_function(m_BlobSeer, "_get_page_size_",reinterpret_cast<VALUE (*)(...)>(method_get_page_size), 1);
	rb_define_module_function(m_BlobSeer, "_append_", reinterpret_cast<VALUE (*)(...)>(method_append), 3);
	rb_define_module_function(m_BlobSeer, "_get_version_", reinterpret_cast<VALUE (*)(...)>(method_get_version), 1);
	rb_define_singleton_method(c_CppObjectHandler,"new",reinterpret_cast<VALUE (*)(...)>(method_init_object_handler),1);
}

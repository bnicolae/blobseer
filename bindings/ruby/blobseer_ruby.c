// Include the blobseer headers
#include "lib/blobseer.h"
// Include the Ruby headers
#include "ruby.h"

// Defining a space for information and references about the class to be stored internally
VALUE c_Blob_t = Qnil;
VALUE c_BlobEnv  = Qnil;
VALUE m_BlobSeer = Qnil;

// Prototype for the initialization method
void Init_blobseer();

// Prototype of the functions
VALUE method_blob_init(VALUE self, VALUE rb_str_config);
VALUE method_blob_finalize(VALUE self, VALUE rbenv);
VALUE method_blob_create(VALUE self, VALUE rbenv, VALUE rbpsize, VALUE rbreplica);
VALUE method_blob_read(VALUE self, VALUE rbblob, VALUE rbversion, VALUE rboffset, VALUE rbsize);
VALUE method_blob_write(VALUE self, VALUE rbblob, VALUE rboffset, VALUE rbsize, VALUE rbstr);
VALUE method_blob_setid(VALUE self, VALUE rbenv, VALUE rbid, VALUE rbblob);
VALUE method_blob_getid(VALUE self, VALUE rbblob);
VALUE method_blob_getlatest(VALUE self, VALUE rbblob);
VALUE method_blob_getversion(VALUE self, VALUE rbblob);
VALUE method_blob_getpagesize(VALUE self, VALUE rbblob);
VALUE method_blob_getsize(VALUE self, VALUE rbblob, VALUE rbversion);
VALUE method_blob_getcurrentsize(VALUE self, VALUE rbblob);
VALUE method_blob_append(VALUE self, VALUE rbblob, VALUE rbsize, VALUE rbbuffer);

VALUE method_blob_t_init(VALUE self);

// The initialization method for the module 
void Init_libblobseer_ruby() {
	// initializes objects
	m_BlobSeer = rb_define_module("BLOBSEER");
	c_Blob_t = rb_define_class_under(m_BlobSeer, "Blob_t", rb_cObject);
	c_BlobEnv = rb_define_class_under(m_BlobSeer,"BlobEnv", rb_cObject);
	// initializes methods
	rb_define_module_function(m_BlobSeer, "_init_", method_blob_init, 1);
	rb_define_module_function(m_BlobSeer, "_finalize_", method_blob_finalize, 1);
	rb_define_module_function(m_BlobSeer, "_create_", method_blob_create, 3);
	rb_define_module_function(m_BlobSeer, "_read_", method_blob_read, 4);
	rb_define_module_function(m_BlobSeer, "_write_", method_blob_write, 4);
	rb_define_module_function(m_BlobSeer, "_setid_", method_blob_setid, 3);
	rb_define_module_function(m_BlobSeer, "_getid_", method_blob_getid, 1);
	rb_define_module_function(m_BlobSeer, "_getsize_", method_blob_getsize, 2);
	rb_define_module_function(m_BlobSeer, "_getcurrentsize_", method_blob_getcurrentsize, 1);
	rb_define_module_function(m_BlobSeer, "_getlatest_", method_blob_getlatest, 1);
	rb_define_module_function(m_BlobSeer, "_getpagesize_", method_blob_getpagesize, 1);
	rb_define_module_function(m_BlobSeer, "_append_", method_blob_append, 3);
	rb_define_module_function(m_BlobSeer, "_getversion_", method_blob_getversion, 1);
	rb_define_singleton_method(c_Blob_t,"new",method_blob_t_init,0);
}

static void blob_t_free(blob_t *b)
{
	// do nothing 
}
static void blob_t_mark(blob_t *b)
{
}
static void env_t_free(blob_env_t *env)
{
	// do nothing
}

// Function for initialisation of blobseer
VALUE method_blob_init(VALUE self, VALUE rb_str_config)
{
	blob_env_t* env = (blob_env_t*)malloc(sizeof(blob_env_t));
	blob_init(RSTRING(rb_str_config)->ptr, env);
	VALUE rbenv = Data_Wrap_Struct(c_BlobEnv, NULL,env_t_free, env);
	return rbenv;
}

// Function for closing blobseer
VALUE method_blob_finalize(VALUE self, VALUE rbenv)
{
	blob_env_t* env;
	Data_Get_Struct(rbenv, blob_env_t, env);
	blob_finalize(env);
	return Qnil;
}

// Creation of a blob
VALUE method_blob_create(VALUE self, VALUE rbenv, VALUE rbpsize, VALUE rbreplica)
{
	offset_t page_size = (uint64_t)NUM2LL(rbpsize);
	unsigned int replica = NUM2UINT(rbreplica);
	blob_t *blob = (blob_t*)malloc(sizeof(blob_t));
	blob_env_t *env;
	Data_Get_Struct(rbenv, blob_env_t, env);
	if(blob_create(env, page_size, replica, blob))
	{
		VALUE rbblob = Data_Wrap_Struct(c_Blob_t, NULL, blob_t_free, blob);
		return rbblob;
	}
	else
	{
		return Qnil;
	}
}

// Reading operation
VALUE method_blob_read(VALUE self, VALUE rbblob, VALUE rbversion, VALUE rboffset, VALUE rbsize)
{
	offset_t offset = (uint64_t)NUM2LL(rboffset);
	offset_t size = (uint64_t)NUM2LL(rbsize);
	uint32_t version = (uint32_t)NUM2UINT(rbversion);
	char* buffer = (char*)calloc(size,sizeof(char));
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	if(blob_read(blob, version, offset, size, buffer))
	{
		VALUE rbstr = rb_str_new(buffer,size);
		free(buffer);
		return rbstr;
	}
	else
	{
		return Qnil;
	}
}

// Writing operation
VALUE method_blob_write(VALUE self, VALUE rbblob, VALUE rboffset, VALUE rbsize, VALUE rbstr)
{
	offset_t offset = (uint64_t)NUM2LL(rboffset);
	offset_t size = (uint64_t)NUM2LL(rbsize);
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	if(blob_write(blob, offset, size, RSTRING(rbstr)->ptr))
	{
		return Qtrue;
	}
	else
	{
		return Qfalse;
	}
}

// Set the id of the blob we want to work on
VALUE method_blob_setid(VALUE self, VALUE rbenv, VALUE rbid, VALUE rbblob)
{
	blob_env_t *env;
	Data_Get_Struct(rbenv, blob_env_t, env);
	unsigned int id = NUM2UINT(rbid);
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	if(blob_setid(env,id,blob))
	{
		rbblob = Data_Wrap_Struct(c_Blob_t, NULL, free, blob);
		return rbblob;
	}
	else
	{
		return Qnil;
	}
}

// Get the latest version of the blob
VALUE method_blob_getlatest(VALUE self, VALUE rbblob)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	if(blob_getlatest(blob))
	{
		return Qtrue;
	}
	else
	{
		return Qfalse;
	}
}

// Get the size of a version of the blob
VALUE method_blob_getsize(VALUE self, VALUE rbblob, VALUE rbversion)
{
	uint32_t version = NUM2UINT(rbversion);
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint64_t size;
	size = blob_getsize(blob, version);
	return UINT2NUM(size);
}

// Get the size of the current version of the blob
VALUE method_blob_getcurrentsize(VALUE self, VALUE rbblob)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint32_t version = blob->latest_version;
	uint64_t size;
	size = blob_getsize(blob, version);
	return UINT2NUM(size);
}

// Append memory to the blob
VALUE method_blob_append(VALUE self, VALUE rbblob, VALUE rbsize, VALUE rbbuffer)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint64_t size = NUM2LL(rbsize);
	if(blob_append(blob, size, STR2CSTR(rbbuffer)))
	{
		return Qtrue;
	}
	else
	{
		return Qfalse;
	}
}

// Get the current version of the blob
VALUE method_blob_getversion(VALUE self, VALUE rbblob)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint64_t version = blob->latest_version;
	return LL2NUM(version);
}

// Constructor of the Blob_t class
VALUE method_blob_t_init(VALUE self)
{
	blob_t *blob  = (blob_t*)malloc(sizeof(blob_t));
	VALUE rbblob = Data_Wrap_Struct(c_Blob_t, blob_t_mark,blob_t_free, blob);
	return rbblob;
}

// Return the page size of a given blob
VALUE method_blob_getpagesize(VALUE self, VALUE rbblob)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint64_t page_size = blob->page_size;
	return LL2NUM(page_size);
}

// Return the page size of a given blob
VALUE method_blob_getid(VALUE self, VALUE rbblob)
{
	blob_t *blob;
	Data_Get_Struct(rbblob, blob_t, blob);
	uint64_t id = blob->id;
	return LL2NUM(id);
}

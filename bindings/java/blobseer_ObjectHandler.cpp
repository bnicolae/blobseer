#include "blobseer_ObjectHandler.h"
#include "client/object_handler.hpp"

#include "common/debug.hpp"

void __attribute__ ((constructor)) blobseer_init() {
}
void __attribute__ ((destructor)) blobseer_fini() {
}

JNIEXPORT jlong JNICALL Java_blobseer_ObjectHandler_blob_1init(JNIEnv *env, jclass c, jstring jcfg) {
    jlong ret = 0;
    const char *cfg = env->GetStringUTFChars(jcfg, NULL);
    std::string cfg_file(cfg);
    env->ReleaseStringUTFChars(jcfg, cfg);
    try {
	object_handler *h = new object_handler(cfg_file);
	ret = (jlong)h;
    } catch (std::runtime_error &e) {
	ERROR(e.what());
    }
    return ret;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_blob_1free(JNIEnv *env, jclass c, jlong blob) {
    if (blob == 0)
	return false;
    delete (object_handler *)blob;
    return true;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_blob_1create(JNIEnv *env, jclass c, jlong blob, jlong ps, jint rc) {
    return ((object_handler*)blob)->create(ps,rc);
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_get_1latest(JNIEnv *env, jclass c, jlong blob, jint blob_id ) {
    return ((object_handler*)blob)->get_latest(blob_id);
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_read(JNIEnv *env, jclass c, jlong blob, jlong offset, jlong size, jobject jbuffer, jint version) {
    char* buffer = (char *)env->GetDirectBufferAddress(jbuffer);
    bool result = false;

    if (buffer == NULL) {
	ERROR("could not get buffer address for reading (" << offset << ", " << size << ") from blob " << blob);
	return false;
    }    
    try {
	result = ((object_handler*)blob)->read(offset, size, buffer, version);
    } catch (std::runtime_error &e) {	
	ERROR(e.what());
	return false;
    }
    return result;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_append(JNIEnv *env, jclass c, jlong blob, jlong size, jobject jbuffer) {
    char* buffer = (char *)env->GetDirectBufferAddress(jbuffer);
    bool result = false;

    if (buffer == NULL) {
	ERROR("could not get buffer address for appending  " << size << "bytes to blob " << blob);
	return false;
    }    
    try {
	result = ((object_handler*)blob)->append(size, buffer);
    } catch (std::runtime_error &e) {	
	ERROR(e.what());
	return false;
    }
    return result;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_write(JNIEnv *env, jclass c, jlong blob, jlong offset, jlong size, jobject jbuffer) {
    char* buffer = (char *)env->GetDirectBufferAddress(jbuffer);
    bool result = false;

    if (buffer == NULL) {
	ERROR("could not get buffer address for writing (" << offset << ", " << size << ") to blob " << blob);
	return false;
    }
    try {
	result = ((object_handler*)blob)->write(offset, size, buffer);
    } catch (std::runtime_error &e) {	
	ERROR(e.what());
	return false;
    }
    return result;
}

JNIEXPORT jint JNICALL Java_blobseer_ObjectHandler_get_1objcount(JNIEnv *env, jclass c, jlong blob) {
    return ((object_handler*)blob)->get_objcount();
}

JNIEXPORT jlong JNICALL Java_blobseer_ObjectHandler_get_1size(JNIEnv *env, jclass c, jlong blob, jint version) {
    return ((object_handler*)blob)->get_size(version);
}

JNIEXPORT jint JNICALL Java_blobseer_ObjectHandler_get_1version(JNIEnv *env, jclass c, jlong blob) {
    return ((object_handler*)blob)->get_version();
}

JNIEXPORT jint JNICALL Java_blobseer_ObjectHandler_get_1page_1size(JNIEnv *env, jclass c, jlong blob) {
    return ((object_handler*)blob)->get_page_size();
}

JNIEXPORT jint JNICALL Java_blobseer_ObjectHandler_get_1id(JNIEnv *env, jclass c, jlong blob) {
    return ((object_handler*)blob)->get_id();
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_get_1locations(JNIEnv *env, jclass c, jlong blob, jlong offset, jlong size, jobject result, jint version) {
    object_handler::page_locations_t loc;

    if (!((object_handler*)blob)->get_locations(loc, offset, size, version))
	return false;

    jclass clsvec = env->FindClass("java/util/Vector");
    if (clsvec == NULL) {
	ERROR("Cannot find class: 'java.util.Vector'");
	return false;
    }

    jmethodID madd = env->GetMethodID(clsvec, "add", "(Ljava/lang/Object;)Z");
    if (madd == NULL) {
	ERROR("Cannot load method: 'java.util.Vector:add', signature: '(Ljava/lang/Object;)Z'");
	return false;
    }

    jclass clspl = env->FindClass("blobseer/PageLocation");
    if (clspl == NULL) {
	ERROR("Cannot find class: 'blobseer.PageLocation'");
	return false;
    }

    jmethodID mcons = env->GetMethodID(clspl, "<init>", "(Ljava/lang/String;Ljava/lang/String;JJ)V");
    if (mcons == NULL) {
	ERROR("Cannot load constructor: 'blobseer.PageLocation.PageLocation', signature: '(Ljava/lang/String;Ljava/lang/String;JJ)V'");
	return false;
    }

    for (unsigned int i = 0; i < loc.size(); i++)
	env->CallBooleanMethod(result, madd, env->NewObject(clspl, mcons, env->NewStringUTF(loc[i].host.c_str()), env->NewStringUTF(loc[i].port.c_str()), 
							    loc[i].offset, loc[i].size));

    return true;
}

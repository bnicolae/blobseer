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

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_read(JNIEnv *env, jclass c, jlong blob, jlong offset, jlong size, jbyteArray jbuffer, jint version) {
    char *buffer = (char*)(env->GetByteArrayElements(jbuffer, 0));
    bool result = ((object_handler*)blob)->read(offset, size, buffer, version);
    env->ReleaseByteArrayElements(jbuffer, (jbyte*)buffer, 0);

    return result;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_append(JNIEnv *env, jclass c, jlong blob, jlong size, jbyteArray jbuffer) {
    char *buffer = (char*)(env->GetByteArrayElements(jbuffer, 0));
    bool result = ((object_handler*)blob)->append(size, buffer);
    env->ReleaseByteArrayElements(jbuffer, (jbyte*)buffer, 0);

    return result;
}

JNIEXPORT jboolean JNICALL Java_blobseer_ObjectHandler_write(JNIEnv *env, jclass c, jlong blob, jlong offset, jlong size, jbyteArray jbuffer) {
    char *buffer = (char*)(env->GetByteArrayElements(jbuffer, 0));
    bool result = ((object_handler*)blob)->write(offset, size, buffer);
    env->ReleaseByteArrayElements(jbuffer, (jbyte*)buffer, 0);

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

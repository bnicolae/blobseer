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

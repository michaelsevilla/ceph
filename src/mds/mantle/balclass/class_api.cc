// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "common/debug.h"

#include "mds/mantle/balclass/balclass.h"

#include "osd/ClassHandler.h"

static ClassHandler *ch;

#define dout_subsys ceph_subsys_mds

void cls_initialize(ClassHandler *h)
{
  ch = h;
}

void cls_finalize()
{
  ch = NULL;
}


void *cls_alloc(size_t size)
{
  return malloc(size);
}

void cls_free(void *p)
{
  free(p);
}

int cls_register(const char *name, cls_handle_t *handle)
{
  ClassHandler::ClassData *cls = ch->register_class(name);
  *handle = (cls_handle_t)cls;
  return (cls != NULL);
}

int cls_unregister(cls_handle_t handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)handle;
  ch->unregister_class(cls);
  return 1;
}

int cls_register_method(cls_handle_t hclass, const char *method,
                        int flags,
                        cls_method_call_t class_call, cls_method_handle_t *handle)
{
  if (!(flags & (CLS_METHOD_RD | CLS_METHOD_WR)))
    return -EINVAL;
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)hclass;
  cls_method_handle_t hmethod =(cls_method_handle_t)cls->register_method(method, flags, class_call);
  if (handle)
    *handle = hmethod;
  return (hmethod != NULL);
}

int cls_register_cxx_method(cls_handle_t hclass, const char *method,
                            int flags,
                cls_method_cxx_call_t class_call, cls_method_handle_t *handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)hclass;
  cls_method_handle_t hmethod = (cls_method_handle_t)cls->register_cxx_method(method, flags, class_call);
  if (handle)
    *handle = hmethod;
  return (hmethod != NULL);
}

int cls_unregister_method(cls_method_handle_t handle)
{
  ClassHandler::ClassMethod *method = (ClassHandler::ClassMethod *)handle;
  method->unregister();
  return 1;
}



int cls_log(int level, const char *format, ...)
{
   int size = 256;
   va_list ap;
   while (1) {
     char buf[size];
     va_start(ap, format);
     int n = vsnprintf(buf, size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196
     if ((n > -1 && n < size) || size > MAX_SIZE) {
       dout(level) << buf << dendl;
       return n;
     }
     size *= 2;
   }
}

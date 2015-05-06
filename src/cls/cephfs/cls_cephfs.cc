// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template from implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

#include <string>
#include <errno.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(cephfs_size_scan)

cls_handle_t h_class;
cls_method_handle_t h_set_if_greater;

/**
 * Set a named xattr to a given integer, if and only if the xattr
 * is not already set to a greater integer.
 *
 * If the xattr is missing, or does not encode an integer, then
 * it is set to the input integer.
 *
 * On success, the output buffer is populated with the resulting
 * integer contained in the xattr.  On failure, a nonzero value is
 * returned and the contents of the output buffer are undefined.
 *
 * @param in: encoded xattr name, uint64_t
 * @param out: the resulting value of the named xattr
 */
static int set_if_greater(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  assert(in != NULL);
  assert(out != NULL);

  std::string xattr_name;
  uint64_t input_val = 0;

  // Decode `in`
  bufferlist::iterator q = in->begin();
  try {
    ::decode(xattr_name, q);
    ::decode(input_val, q);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // Load existing xattr value
  uint64_t existing_val = 0;
  bufferlist existing_bl;
  bool set_it = false;
  int r = cls_cxx_getxattr(hctx, xattr_name.c_str(), &existing_bl);
  if (r == -ENOENT || existing_bl.length() == 0) {
    set_it = true;
  } else if (r == 0) {
    bufferlist::iterator existing_p = existing_bl.begin();
    try {
      ::decode(existing_val, existing_p);
      if (!existing_p.end()) {
        // Trailing junk?  Consider it invalid and overwrite
        set_it = true;
      } else {
        // Valid existing value, do comparison
        set_it = input_val > existing_val;
      }
    } catch (const buffer::error &err) {
      // Corrupt or empty existing value, overwrite it
      set_it = true;
    }
  } else {
    return r;
  }

  // Conditionally set the new xattr
  uint64_t result_val;
  if (set_it) {
    bufferlist set_bl;
    ::encode(input_val, set_bl);
    r = cls_cxx_setxattr(hctx, xattr_name.c_str(), &set_bl);
    if (r < 0) {
      return r;
    }
    result_val = input_val;
  } else {
    result_val = existing_val;
  }

  // Encode the result
  ::encode(result_val, *out);

  return 0;
}

/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
void __cls_init()
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cephfs_size_scan");

  cls_register("cephfs", &h_class);
  cls_register_cxx_method(h_class, "set_if_greater",
			  CLS_METHOD_WR | CLS_METHOD_RD,
			  set_if_greater, &h_set_if_greater);
}


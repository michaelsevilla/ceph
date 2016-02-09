#include "lua_core/lua_core.h"
#include "mds/mantle/balclass/balclass.h"

CLS_VER(1,0)
CLS_NAME(lua)

/*
 * Entrypoint for the shared lib upon dlopen
 */
void __cls_init()
{
  CLS_LOG(20, "Loaded lua balancer class!");

  /*
   * Functions to add to clslua_lib
   */
  list<luaL_Reg> functions;

  /*
   * Lua files to add to the environment
   */
  list<string> env;
  env.push_back("libmantle.lua");

  cls_init("bal_lua", "balclass", functions, env);
}

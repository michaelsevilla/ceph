#include "mdstypes.h"
#include "MDSRank.h"
#include "Mantle.h"
#include "msg/Messenger.h"

#include <fstream>

#define dout_subsys ceph_subsys_mds_balancer
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds.mantle "

int dout_wrapper(lua_State *L)
{
  #undef dout_prefix
  #define dout_prefix *_dout << "lua.balancer "

  /* Lua indexes the stack from the bottom up */
  int bottom = -1 * lua_gettop(L);
  if (!lua_isinteger(L, bottom)) {
    dout(0) << "WARNING: BAL_LOG has no message" << dendl;
    return -EINVAL;
  }

  /* bottom of the stack is the log level */
  int level = lua_tointeger(L, bottom);

  /* rest of the stack is the message */
  string s = "";
  for (int i = bottom + 1; i < 0; i++)
    lua_isstring(L, i) ? s.append(lua_tostring(L, i)) : s.append("<empty>");

  dout(level) << s << dendl;
  return 0;
}



void Mantle::push_metrics(lua_State *L) {
  /* table to hold all metrics */
  lua_newtable(L);

  /* fill in the metrics for each mds by grabbing load struct */
  for (mds_rank_t i=mds_rank_t(0);
       i < mds_rank_t(mds->get_mds_map()->get_num_in_mds());
       i++) {
    map<mds_rank_t, mds_load_t>::value_type val(i, mds_load_t(ceph_clock_now(g_ceph_context)));
    std::pair < map<mds_rank_t, mds_load_t>::iterator, bool > r(mds_load.insert(val));
    mds_load_t &load(r.first->second);

    /* this mds has an associated table of metrics */
    lua_pushinteger(L, i);
    lua_newtable(L);

    /* push metric; setfield assigns key and pops the val */
    lua_pushnumber(L, load.auth.meta_load());
    lua_setfield(L, -2, "auth.meta_load");
    lua_pushnumber(L, load.all.meta_load());
    lua_setfield(L, -2, "all.meta_load");
    lua_pushnumber(L, load.req_rate);
    lua_setfield(L, -2, "req_rate");
    lua_pushnumber(L, load.queue_len);
    lua_setfield(L, -2, "queue_len");
    lua_pushnumber(L, load.cpu_load_avg);
    dout(0) << "cpu_load_avg=" << load.cpu_load_avg << dendl;
    lua_setfield(L, -2, "cpu_load_avg");

    /* in the table at at stack[-3], set k=stack[-1] to v=stack[-2] */
    lua_rawset(L, -3);
  }

  /* global mds table exposed to Lua */
  lua_setglobal(L, "mds");
}



int Mantle::balance(MDSRank *m, map<mds_rank_t,double> my_targets)
{
  mds = m;

  /* load script from localfs */
  ifstream t("/tmp/test.lua");
  string script((std::istreambuf_iterator<char>(t)),
                 std::istreambuf_iterator<char>()); 

  /* build lua vm state */
  lua_State *L = luaL_newstate(); 
  if (!L) {
    dout(0) << "WARNING: mantle could not load Lua state" << dendl;
    return -ENOEXEC;
  }

  /* balancer policies can use basic Lua functions */
  luaopen_base(L);

  /* load the balancer */
  if (luaL_loadstring(L, script.c_str())) {
    dout(0) << "WARNING: mantle could not load balancer: "
            << lua_tostring(L, -1) << dendl;
    lua_close(L);
    return -EINVAL;
  }

  /* setup debugging and put metrics into a table */
  lua_register(L, "BAL_LOG", dout_wrapper);
  push_metrics(L);

  /* compile/execute balancer */
  int ret = lua_pcall(L, 0, LUA_MULTRET, 0);

  if (ret) {
    dout(0) << "WARNING: mantle could not execute script: "
            << lua_tostring(L, -1) << dendl;
    lua_close(L);
    return -EINVAL;
  }

  if (lua_istable(L, -1) == 0 ||
      mds->get_mds_map()->get_num_in_mds() != lua_rawlen(L, -1)) {
    dout(0) << "WARNING: mantle script returned a malformed response" << dendl;
    lua_close(L);
    return -EINVAL;
  }

  /* parse response by iterating over Lua stack */
  mds_rank_t it = mds_rank_t(0);
  lua_pushnil(L);
  while (lua_next(L, -2) != 0) {
    my_targets[it] = (lua_tointeger(L, -1));
    lua_pop(L, 1);
    it++;
  }

  lua_close(L);

  return 0;
}

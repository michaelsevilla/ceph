#include <lua.hpp>
#include <list>
#include <map>
using std::list;
using std::map;

#include "include/types.h"
#include "common/Clock.h"
#include "CInode.h"



class MDSRank;
class Message;
class MHeartbeat;
class CInode;
class CDir;
class Messenger;
class MonClient;

class Mantle {
  protected:
    MDSRank *mds;
    map<mds_rank_t, mds_load_t>  mds_load;
    void push_metrics(lua_State *L);

  public:
//MDSRank *m, map<mds_rank_t,double> mt) : mds(m), my_targets(mt) {}
    int balance(MDSRank *m, map<mds_rank_t,double> my_tagets);
};

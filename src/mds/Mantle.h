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
    lua_State *L;
    map<mds_rank_t, mds_load_t>  mds_load;

  public:
    Mantle() : L(NULL) {};
    ~Mantle() 
    {
      if (L == NULL) 
        return;
      lua_close(L);
    };
    int start();
    void expose_metrics(string name, vector < map<string, double> > metrics);
    int execute(map<mds_rank_t,double> &my_targets);
};

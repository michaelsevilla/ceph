function rebalance(input, output)
  ctx = {}
  ctx["log"], ctx["whoami"], ctx["MDSs"], ctx["targets"] = parse_args(input:str())
  ctx["whoami"] = ctx["whoami"] + 1

  MDSs = ctx["MDSs"]
  for i=1,#MDSs do
    ctx["i"] = i
    MDSs[i]["load"] = mds_load(ctx)
  end
----  total = libmantle.get_total(MDSs)
  print_metrics(ctx)




--  if when(MDSs, whoami) then
--     E, I = libmantle.get_exporters_importers(log, MDSs)
--  else
--     return libmantle.get_empty_targets(log, targets)
--  end
--  tic const char *LUA_WHERE = "\n"
--  return libmantle.convert_targets(log, targets)";
--  
--  tic const char *LUA_INIT_HOWMUCH = "?.lua;'\n require \"libbinpacker\"\n"
--  strategies = ";
--  tic const char *LUA_HOWMUCH = "\n"
--  return libbinpacker.pack(strategies, arg)";
  output:append("0 0 0\0")
end

function mds_load(ctx)
  i = ctx["i"]
  return ctx["MDSs"][i]["all"]
end

function when(MDSs, whoami)
  return MDSs[whoami]["load"] > 0.01 and MDSs[whoami+1]["load"] < 0.01
end

objclass.register(rebalance)


-- package.path = package.path .. ';/usr/local/share/ceph/rados-classes/?.lua';
-- require "libmantle"

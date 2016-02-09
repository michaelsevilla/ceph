FUDGE = 0.001
METRICS = {"auth", "all", "req", "q", "cpu", "mem"}

-- Print out the metric measurements for each MDS.
-- @arg0:  dictionary of MDS loads
function print_metrics(ctx)
  MDSs = ctx["MDSs"]
  
  str = ""
  for i=1, #METRICS do
    str = str .. " " .. METRICS[i]
  end
  balclass.log(0, "load metrics: whoami=" .. whoami .. " <" .. str .. " >")

  for i=1, #MDSs do
    str = "  MDS" .. i-1 .. " load=" .. MDSs[i]["load"] .. " <"
    for j=1, #METRICS do
      str = str .. " " .. MDSs[i][METRICS[j]]
    end
   balclass.log(0, str .. " >")
  end
end

-- Parse the arguments passed from C++ Ceph code. For this version of the
-- API, the order of the arguments is
--     me, 6-tuples for each MDS
--     (metadata load on authority, metadata on all other subtrees, 
--     request rate, queue length, CPU load, memory load)
-- @arg0:  balancing context; a data structure with all metrics
--         variables, and a variable (e.g., targets) representing
--         the final decision
-- @arg1:  the string to parse
-- return: array mapping MDSs to load maps
function parse_args(ctx, input)
  args = {}
  for arg in input:gmatch("%S+") do 
    table.insert(args, arg)
  end

  whoami = tonumber(args[1])
  metrics = {}
  if (#args - 1) % #METRICS ~= 0 then
    balclass.log(0, "Didn't receive all load metrics for all MDSs")
    return -1
  end

  balclass.log(20, "Fill in the metrics array, which lines up with MDSs array")
  i = 1
  for k,v in ipairs(args) do
    if k > 1 then
      metrics[i] = v
      i = i + 1
    end 
  end

  balclass.log(20, "Copy the values into the MDSs array as numbers")
  MDSs = {}
  nmds = 0
  for i=0, #metrics-1 do
    if i % #METRICS == 0 then 
      nmds = nmds + 1
      MDSs[nmds] = {}
      MDSs[nmds]["load"] = 0
    end
    MDSs[nmds][METRICS[(i % #METRICS) + 1]] = tonumber(metrics[i+1])
    i = i + 1
  end
  targets = {}
  for i=1,#MDSs do 
    targets[i] = 0
  end

  ctx["whoami"] = whoami + 1
  ctx["MDSs"] = MDSs
  ctx["targets"] = targets
  return ctx
end

-- Save the state for future migration decisions
-- @arg0:  string to save
function wr_state(x)
  stateF = io.open("/tmp/balancer_state", "w")
  stateF:write(x)
  io.close(stateF)
end

-- Read the state saved by a previous migration decision
-- return: string of the saved value
function rd_state()
  stateF = io.open("/tmp/balancer_state", "r")
  state = stateF:read("*all")
  io.close(stateF)
  return state
end

-- Basic min/max functions 
-- @arg0:  first number
-- @arg1:  second number
-- return: the smaller/larger number
function max(x, y) if x > y then return x else return y end end
function min(x, y) if x < y then return x else return y end end

-- Get the total load, which is used to determine if the current MDS 
-- is overloaded and for designating importer and exporter MDSs.
-- @arg0:  dictionary of MDS loads
-- return: the sum of all the MDS loads
function get_total(ctx)
  total = 0
  for i=1,#ctx["MDSs"] do
    total = total + ctx["MDSs"][i]["load"]
  end
  return total
end

-- Return an empty array of targets, used when the ``when''
-- condition fails.
-- @arg0:  dictionary of MDS loads
-- return: array of 0s
function get_empty_targets()
  balclass.log(0, "--> not migrating")
  ret = ""
  for i=1,#targets do 
    ret = ret..targets[i].." " 
  end
  return ret
end

-- Convert the array of targets into a string; zero out targets
-- if assignment is illegal
-- @arg0:  array of loads 
-- return: string of targets
function convert_targets(targets)
  ret = ""
  if targets[whoami] ~= 0 then
    for i=1,#targets do 
      targets[i] = 0 
    end
  end
  for i=1,#targets do 
    ret = ret..targets[i].." " 
  end
  return ret
end

-- Sanity check the targets filled in by the custom balancer and return
-- the targetes array "regardless of it erred out" as a string.
-- @arg0: balancing context
function check_targets(targets)
  ret = ""
  if targets[whoami] ~= 0 then
    balclass.log(0, "  Uh oh... trying to send load to myself, zeroing out targets array.\n")
    for i=1,#targets do targets[i] = 0 end
  else
    for i=1,#targets do ret = ret..targets[i].." " end
  end
  return ret
end

-- Designate MDSs as importers (those who have the capacity for load)
-- and exporters (thsoe that want to shed laod). This is modeled off
-- of Sage's original work sharing design.
-- @arg0:  balancing context
-- return: an importer and exporter array
function get_exporters_importers(ctx)
  MDSs = ctx["MDSs"]
  total = get_total(ctx)
  balclass.log(0, "--> migrating! (total load=" .. total .. ")")
  E = {}; I = {}
  for i=1,#MDSs do
    metaload = MDSs[i]["load"]
    if metaload > total / #MDSs then 
      E[i] = metaload
    else 
      I[i] = metaload 
    end
  end
  return E, I
end

-- Entrypoint for the metadata blancer. This is the general balancing
-- framework that can be molded into Greedy Spill, Greedy Spill Even,
-- Fill and Spill, and the Adapatable balancers, given the right policies.
-- @arg0:  the metrics passed in as a bufferlist (crude, I know)
-- @arg1:  the balancing decision, passed back as bufferlist
-- return: 0 on success, -1 on error
function rebalance(input, output)
  balclass.log(20, "input=" .. input:str())

  ctx = {}
  if (parse_args(ctx, input:str()) == -1) then
    return -1
  end

  balclass.log(10, "apply the user-defined MDS_LOAD policy to all MDSs")
  MDSs = ctx["MDSs"]
  for i=1,#MDSs do
    ctx["i"] = i
    MDSs[i]["load"] = MDS_LOAD(ctx)
  end
  print_metrics(ctx)

  total = get_total(ctx)
  balclass.log(10, "sum of all loads total=" .. total)

  balclass.log(10, "apply the user-defined WHEN policy")
  if WHEN(ctx) then
     E, I = get_exporters_importers(ctx)
     balclass.log(10, "apply the user-defined WHERE policy")
     WHERE(ctx)
     output:append(convert_targets(ctx["targets"]))
  end
  return 0
end



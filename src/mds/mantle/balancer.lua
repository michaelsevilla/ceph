metrics = {"auth.meta_load", "all.meta_load", "req_rate", "queue_len", "cpu_load_avg"}
for i=0, #mds do
  s = "MDS"..i..": < "
  for j=1, #metrics do
    s = s..metrics[j].."="..mds[i][metrics[j]].." "
  end
  BAL_LOG(0, s..">")
end

return {3, 4, 5}

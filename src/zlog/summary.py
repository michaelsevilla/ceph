#!/usr/bin/python
import sys
import re
import glob
from collections import defaultdict

def get_experiments():
    summary = defaultdict(lambda: [])
    for fn in glob.glob("*.[0-9]*.[0-9]*"):
        tag, ts, pid = fn.split(".")
        instance = (tag, long(ts))
        summary[instance].append((pid, fn))
    return summary

def summarize_experiment(instances):
    total_ops = 0.0
    total_lat = 0.0
    num_instances = len(instances)
    for pid, filename in instances:
        f = open(filename)
        avg_ops = float(f.readline().strip())
        avg_lat = float(f.readline().strip())
        total_ops += avg_ops
        total_lat += avg_lat
        print "pid", pid, "average ops", avg_ops
    print "total ops", total_ops, \
          "average lat", total_lat/num_instances, \
          "average client ops", total_ops/num_instances

def print_summary(instance, results, prefix=""):
    title = "%sexperiment: tag=%s time=%s" % \
        (prefix, instance[0], instance[1])
    sep = "=" * len(title)
    print "%s\n%s\n%s" % (sep, title, sep)
    summarize_experiment(results)
    print ""

experiments = get_experiments()
sorted_keys = sorted(experiments.keys(), key=lambda k: k[1])
experiments = map(lambda k: (k, experiments[k]), sorted_keys)

for instance, results in experiments[:-1]:
    print_summary(instance, results)
print_summary(*experiments[-1], prefix="LATEST ")

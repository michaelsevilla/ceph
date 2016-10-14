#define _FILE_OFFSET_BITS 64
#include <cassert>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <random>
#include <iostream>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <boost/program_options.hpp>
#include "include/cephfs/libcephfs.h"

namespace po = boost::program_options;

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

static volatile int stop;
static std::atomic_ullong ios;

static uint64_t run_start_ns;
static uint64_t run_end_ns;

static void sigint_handler(int sig)
{
  stop = 1;
}

// <start, latency>
static std::vector<std::pair<uint64_t, uint64_t>> latencies;

static void run(struct ceph_mount_info *cmount, std::string filename, bool idle, bool record_latency)
{
  int ret = ceph_mkdir(cmount, "/seqdir", 0755);
  if (ret && ret != -EEXIST) {
    std::cerr << "mkdir: " << strerror(-ret) << std::endl;
    assert(0);
  }

  ret = ceph_mkdir(cmount, "/seqdir/seqdir", 0755);
  if (ret && ret != -EEXIST) {
    std::cerr << "mkdir: " << strerror(-ret) << std::endl;
    assert(0);
  }

  std::stringstream ss;
  ss << "/seqdir/seqdir/" << filename;
  std::string path = ss.str();

  std::cout << "path " << path << std::endl;

  int fd = ceph_open(cmount, path.c_str(), O_CREAT|O_RDWR, 0600);
  assert(fd >= 0);

  int r = ceph_set_lseek_target(cmount, fd);
  assert(r == 0);

  // hold the file open but don't do anything
  if (idle) {
    while (!stop) {
      sleep(2);
    }
    ceph_close(cmount, fd);
    return;
  }

  long skip = 200;

  run_start_ns = 0;

  for (;;) {
    uint64_t start = getns();
    int64_t ret = ceph_lseek(cmount, fd, 0, SEEK_END);
    uint64_t latency = getns() - start;
    assert(ret == 0);

    ios++;

    if (stop) {
      run_end_ns = getns();
      break;
    }

    // warm up
    if (skip) {
      skip--;
      continue;
    }

    if (!run_start_ns)
      run_start_ns = getns();

    if (record_latency)
      latencies.emplace_back(start, latency);
  }

  ceph_close(cmount, fd);
}

static void dump_latency(std::string file)
{
  if (file.size() == 0)
    return;

  /*
   * ecdf stuff
   */
  const size_t num_latencies = latencies.size();
  std::sort(latencies.begin(), latencies.end(),
      [](const std::pair<uint64_t, uint64_t>& a,
        const std::pair<uint64_t, uint64_t>& b) {
      return a.second < b.second;
      });

  std::vector<double> ecdf;
  ecdf.reserve(num_latencies);
  for (size_t count = 0; count < num_latencies; count++) {
    double p = (double)count / (double)num_latencies;
    ecdf.push_back(p);
  }

  std::set<size_t> samples;
  samples.insert(0);
  samples.insert(num_latencies-1);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1, num_latencies-2);

  for (int i = 0; i < 1000; i++)
    samples.insert(dis(gen));

  /*
   * throughput
   */
  uint64_t ios = num_latencies;
  uint64_t dur_ns = run_end_ns - run_start_ns;
  double iops = (double)(ios * 1000000000ULL) / (double)dur_ns;

  /*
   * output
   */
  int fd = 0;
  fd = open(file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
  assert(fd > 0);

  // average throughput
  dprintf(fd, "%f\n", iops);

  // latency cdf
  for (auto it = samples.begin(); it != samples.end(); it++) {
    size_t pos = *it;
    dprintf(fd, "%llu,%f\n",
        (unsigned long long)latencies[pos].second,
        ecdf[pos]);
  }

  fsync(fd);
  close(fd);
}

static void report(int period)
{
  ios = 0;
  uint64_t start_ns = getns();
  uint64_t expr_start_ns = start_ns;

  while (1) {
    // length of time to accumulate iops stats
    sleep(period);

    uint64_t period_ios = ios.exchange(0);
    uint64_t end_ns = getns();
    uint64_t dur_ns = end_ns - start_ns;
    start_ns = end_ns;

    // completed ios in prev period
    double iops = (double)(period_ios * 1000000000ULL) / (double)dur_ns;

    uint64_t elapsed_sec = (end_ns - expr_start_ns) / 1000000000ULL;
    std::cout << elapsed_sec << "s: " << "rate=" << (int)iops << " reqs" << std::endl;

    if (stop)
      break;
  }
}

int main(int argc, char **argv)
{
  int report_period;
  int runtime_sec;
  std::string perf_file;
  std::string filename;
  bool idle;
  double capdelay;
  int instances;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("report-sec", po::value<int>(&report_period)->default_value(5), "Report sec")
    ("runtime", po::value<int>(&runtime_sec)->default_value(30), "Runtime sec")
    ("perf_file", po::value<std::string>(&perf_file)->default_value(""), "Perf file")
    ("filename", po::value<std::string>(&filename)->default_value("seq"), "Filename")
    ("idle", po::value<bool>(&idle)->default_value(false), "Idle")
    ("capdelay", po::value<double>(&capdelay)->default_value(0.0), "cap delay")
    ("instances", po::value<int>(&instances)->default_value(1), "instances")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  assert(instances > 0);
  pid_t pids[instances];

  time_t ts = time(NULL);

  if (perf_file.size()) {
    std::stringstream ss;
    ss << perf_file << "." << ts;
    perf_file = ss.str();
  }

  if (instances > 1) {
    for (int i = 1; i < instances; i++) {
      pid_t pid = fork();
      if (pid > 0) {
        pids[i-1] = pid;
        continue;
      } else if (pid == 0) {
        break;
      } else if (pid == -1) {
        perror("fork");
        exit(1);
      } else
        assert(0);
    }
  }

  /*
   * Connect to CephFS
   */
  struct ceph_mount_info *cmount;

  int ret = ceph_create(&cmount, "admin");
  assert(ret == 0);

  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_env(cmount, NULL);

  ret = ceph_mount(cmount, "/");
  assert(ret == 0);

  if (perf_file.size()) {
    std::stringstream ss;
    ss << perf_file << "." << getpid();
    perf_file = ss.str();
    latencies.reserve(20000000);
  }

  std::cout << "Running for " << runtime_sec << " seconds" << std::endl;

  assert(signal(SIGINT, sigint_handler) != SIG_ERR);

  stop = 0;

  if (capdelay > 0.0)
    ceph_set_cap_handle_delay(cmount, capdelay);

  /*
   * Start workload
   */
  std::thread runner{[&] {
    run(cmount, filename, idle, perf_file.size() > 0);
  }};

  std::thread reporter{[&] {
    report(report_period);
  }};

  sleep(runtime_sec);
  stop = 1;

  runner.join();
  reporter.join();

  dump_latency(perf_file);

  /*
   * Clean-up connections...
   */
  ceph_unmount(cmount);
  ceph_release(cmount);

  if (instances > 1) {
    for (int i = 0; i < instances; i++) {
      waitpid(pids[i], NULL, 0);
    }
  }

  return 0;
}

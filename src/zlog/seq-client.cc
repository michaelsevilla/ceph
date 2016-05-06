#define _FILE_OFFSET_BITS 64
#include <cassert>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <boost/program_options.hpp>
#include <cephfs/libcephfs.h>

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

  // hold the file open but don't do anything
  if (idle) {
    while (!stop) {
      sleep(2);
    }
    ceph_close(cmount, fd);
    return;
  }

  for (;;) {
    uint64_t start = getns();
    //struct stat st;
    //int ret = ceph_stat(cmount, path.c_str(), &st);
    //int ret = ceph_fstat(cmount, fd, &st);
    int64_t ret = ceph_lseek(cmount, fd, 0, SEEK_END);
    uint64_t latency = getns() - start;
    assert(ret == 0);

    ios++;

    if (record_latency)
      latencies.emplace_back(start, latency);

    if (stop)
      break;
  }

  ceph_close(cmount, fd);
}

static void dump_latency(std::string file)
{
  if (file.size() == 0)
    return;

  int fd = 0;
  fd = open(file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0444);
  assert(fd > 0);

  for (auto entry : latencies) {
    dprintf(fd, "%llu %llu\n",
        (unsigned long long)entry.first,
        (unsigned long long)entry.second);
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

  po::options_description desc("Allowed options");
  desc.add_options()
    ("report-sec", po::value<int>(&report_period)->default_value(5), "Report sec")
    ("runtime", po::value<int>(&runtime_sec)->default_value(30), "Runtime sec")
    ("perf_file", po::value<std::string>(&perf_file)->default_value(""), "Perf file")
    ("filename", po::value<std::string>(&filename)->default_value("seq"), "Filename")
    ("idle", po::value<bool>(&idle)->default_value(false), "Idle")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

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

  if (perf_file.size())
    latencies.reserve(20000000);

  std::cout << "Running for " << runtime_sec << " seconds" << std::endl;

  assert(signal(SIGINT, sigint_handler) != SIG_ERR);

  stop = 0;

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

  return 0;
}

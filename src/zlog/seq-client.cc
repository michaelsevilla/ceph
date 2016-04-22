#define _FILE_OFFSET_BITS 64
#include <cassert>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

static void run(struct ceph_mount_info *cmount)
{
  int fd = ceph_open(cmount, "/foo", O_CREAT, 0600);
  assert(fd >= 0);
  ceph_close(cmount, fd);

  for (;;) {
    /*
     * FIXME: ceph_fstat doesn't acquire caps, so it is always a round trip.
     * that's a bug and we can submit a ticket / patch for that so we can stat
     * the file descriptor instead of the full path.
     */
    struct stat st;
    int ret = ceph_stat(cmount, "/foo", &st);
    assert(ret == 0);

    ios++;

    if (stop)
      return;
  }
}

static void report(int period, std::string perf_file)
{
  ios = 0;
  uint64_t start_ns = getns();
  uint64_t expr_start_ns = start_ns;

  int fd = 0;
  if (perf_file.size()) {
    fd = open(perf_file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0444);
    assert(fd > 0);
  }

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

    if (fd) {
      dprintf(fd, "%llu %llu\n",
          (unsigned long long)end_ns,
          (unsigned long long)iops);
    }

    if (stop) {
      if (fd) {
        close(fd);
        fsync(fd);
      }
      break;
    }
  }
}

int main(int argc, char **argv)
{
  int report_period;
  int runtime_sec;
  std::string perf_file;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("report-sec", po::value<int>(&report_period)->default_value(5), "Report sec")
    ("runtime", po::value<int>(&runtime_sec)->default_value(30), "Runtime sec")
    ("perf_file", po::value<std::string>(&perf_file)->default_value(""), "Perf file")
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

  std::cout << "Running for " << runtime_sec << " seconds" << std::endl;

  stop = 0;

  /*
   * Start workload
   */
  std::thread runner{[&] {
    run(cmount);
  }};

  std::thread reporter{[&] {
    report(report_period, perf_file);
  }};

  sleep(runtime_sec);
  stop = 1;

  runner.join();
  reporter.join();

  /*
   * Clean-up connections...
   */
  ceph_unmount(cmount);
  ceph_release(cmount);

  return 0;
}

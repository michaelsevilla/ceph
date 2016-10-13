add_executable(seq-client seq-client.cc)
target_link_libraries(seq-client cephfs pthread
    ${Boost_PROGRAM_OPTIONS_LIBRARY})

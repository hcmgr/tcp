cd build
make
./integration_tests --gtest_filter="${1:-*}.*"

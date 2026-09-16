cd build
make
./unit_tests --gtest_filter="${1:-*}.*"

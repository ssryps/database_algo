//
// Created by mason on 11/29/18.
//

#include "gtest/gtest.h"

#include "test_framework.cpp"
#include "gmock/gmock.h"

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    ::testing::GTEST_FLAG(filter) = "*OccTest*";


    return RUN_ALL_TESTS();
}

extern int benchmark_flag;

class TimestampTest : public testing::Test {
public:
    TimestampTest() {}

};

TEST_F(TimestampTest, Test1) {
    benchmark_flag = SELF_MADE_BENCHMARK;
    PthreadTest(0, NULL, ALGO_TIMESTAMP);

}


class OccTest : public testing::Test {
public:
    OccTest() {}

};

TEST_F(OccTest, occ_pthread_test) {
    benchmark_flag = SELF_MADE_BENCHMARK;
    PthreadTest(0, NULL, ALGO_OCC);

}

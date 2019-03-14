//
// Created by mason on 1/21/19.
//

#include <utils.h>
#include "test_framework.cpp"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

extern bool random_flag;

class TimestampTest : public testing::Test {
public:
    TimestampTest() {}

};

TEST_F(TimestampTest, Test1) {
    random_flag = false;
    PthreadTest(0, NULL, ALGO_TIMESTAMP);


}
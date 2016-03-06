#include "gtest/gtest.h"

TEST(cpp_sorter_test, null_term_str_sort)
{
    char arr[] = "abcdefghab";
    char eq[]  = "aabbcdefgh";
	EXPECT_EQ(arr[0], eq[0]);
}

TEST(cpp_sorter_test, char_arr_sort)
{
    char arr[] = {'a','b','c','d','e','f','g','h','a','b'};
    char eq[]  = {'a','a','b','b','c','d','e','f','g','h'};
    int sz = sizeof(arr)/sizeof(arr[0]);

    for(int i=0; i<sz; i++)
	EXPECT_EQ(arr[i], eq[i]);
}

TEST(cpp_sorter_test, int_arr_sort)
{
    int arr[] = {9,8,7,6,5,4,3,2,1,0};
    int eq[]  = {0,1,2,3,4,5,6,7,8,9};
    int sz = sizeof(arr)/sizeof(arr[0]);

    for(int i=0; i<sz; i++)
	EXPECT_EQ(arr[i], eq[i]);
}

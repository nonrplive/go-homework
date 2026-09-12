// Homework 1: Two Sum solution

package main

import "fmt"

func twoSum(nums []int, target int) []int {
	for i := 0; i < len(nums)-1; i++ {
		for j := i + 1; j < len(nums); j++ {
			if nums[i]+nums[j] == target {
				return []int{i, j}
			}
		}
	}
	return nil
}

func main() {
	fmt.Println(twoSum([]int{2, 6, 7, 15}, 9))
}

package com.littlewool.tech.insight;

/**
 * @ClassName: A
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/21 19:17
 * @Version: 1.0
 **/

class Solution {
    public void moveZeroes(int[] nums) {
        int cout=0;
        for(int i=0;i<nums.length;i++){
            if(nums[i]==0){
                cout++;
            }else{
                swap(i,i-cout,nums);
            }
        }
    }
    private void swap(int i,int j,int[] nums){
        int tmp=nums[i];
        nums[i]=nums[j];
        nums[j]=tmp;
    }
}
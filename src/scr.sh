#!/bin/bash

echo "----------BEFORE TEST-----------------" 
ls -lsh /media/hkc/csd_3/Test_Plog_dir/ 
ls -lsh /media/hkc/csd/TEST_DB_Plog/ 
rm /media/hkc/csd_3/Test_Plog_dir/*
rm /media/hkc/csd/TEST_DB_Plog/*

# test baseline
echo "----------START TEST BASELINE-----------------" 
/home/hkc/Plog/Plog-stable-4.1/Plog-stable-V4.1/testBaseBig 
ls -lsh /media/hkc/csd_3/Test_Plog_dir/ 
echo "----------END OF TEST BASELINE-----------------" 

echo "----------START TEST Plog-----------------" 
/home/hkc/Plog/Plog-stable-4.1/Plog-stable-V4.1/testPlogBig 
ls -lsh /media/hkc/csd/TEST_DB_Plog/ 
echo "----------END OF TEST Plog-----------------" 

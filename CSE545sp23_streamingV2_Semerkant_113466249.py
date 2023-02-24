##########################################################################
## CSE545sp23_streamingV2_Semerkant_113466249.py
## version 2:
##   -- prints stream outputs in more helpful format
##   -- adds element=line.strip() in task1B
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name: Soroush Semerkant
## Student ID: 113466249


import sys
from pprint import pprint
from random import random
from collections import deque
from sys import getsizeof
try:
    import resource
except:
    pass
from math import log, log2 #natural log
import numpy as np
import mmh3 #hashing library

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

MEMORY_SIZE = 1000 #do not edit
#this is the only memory you get; a deque functions just like an array
#it is of size 1,000
memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit



def task1A_meanRGBsStream(element, returnResult = True):
    #[TODO]#
    #procss the element you may only use memory, storing at most 1000
    if memory1a[0] == None:
        memory1a[0] = memory1a[1] = memory1a[2] = memory1a[3]= 0
    memory1a[0]+=1

    element = element[1:-1]
    element = element.split(", ")

    for i, el in enumerate(element):

        memory1a[i+1] += int(el)

    if returnResult: #when the stream is requesting the current result
        result = (0.0, 0.0, 0.0)
        #[TODO]#
        #any additional processing to return the result at this point
        result = (memory1a[1]/memory1a[0], memory1a[2]/memory1a[0],memory1a[3]/memory1a[0])
        
        return result
    else: #no need to return a result
        pass

MEMORY_SIZE = 160000 #do not edit
#this is the only memory you get; a deque functions just like an array
#it is of size 1,000
memory1b =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1B_bloomSetup(elements_in_set):
    #[TODO]#
    #setup the bloom filter memory to be able to filter streaming elements
    bloom_hash_len = memory1b[4] = 2 # number of hash functions
    bloom_hash_start = 5 #starting index of hash functions
    # for i in range(bloom_hash_start, bloom_hash_start+bloom_hash_len): #for each of the hash functions
    #     memory1b[i] = int(random()*1000)  #set the value to be a random int

    # bloom_buckets_len = memory1b[bloom_hash_start+bloom_hash_len] = 100000 #number of buckets in bit array
    # bloom_buckets_start = bloom_hash_start+bloom_hash_len+1 #start of buckets
    bloom_buckets_len = 160000
    bloom_hash_len = 8
    bloom_buckets_start = 0
    for i in range(bloom_buckets_start, bloom_buckets_start+bloom_buckets_len): #clear all buckets to 0
        memory1b[i] = False
    
    for element in elements_in_set: #for each element
        element = element[1:-1] # pull off parenthesis from string
        element = element.split(", ") #split on commas to make array of rgb values
        
        #divide by 100 to allow values to be rounded to the nearest 100 allows for abs(r1 - r2) < 100
        # total = ""
        # for i in element:
        #     total += i 
       
        # for seed in range(bloom_hash_start, bloom_hash_start+bloom_hash_len):
        for seed in range(bloom_hash_len):
            # pos = mmh3.hash(total, memory1b[seed]) % bloom_buckets_len 
            # pos = mmh3.hash(total, seed) % bloom_buckets_len 
            if(seed % 4 == 0):
                pos = mmh3.hash(element[0], seed) % bloom_buckets_len 
            elif(seed % 4 == 1):
                pos = mmh3.hash(element[1], seed) % bloom_buckets_len 
            elif(seed % 4 == 2):
                pos = mmh3.hash(element[2], seed) % bloom_buckets_len 
            # elif(seed % 5 == 3):
            #     pos = mmh3.hash(str( int(element[0]) + int(element[1]) +  int(element[2]) ), seed) % bloom_buckets_len 
            elif(seed % 4 == 3):
                pos = mmh3.hash(element[0] +"," +element[1] + "," +element[2] , seed) % bloom_buckets_len 
            memory1b[bloom_buckets_start+pos] = True
    return 
     
def task1B_bloomStream(element):
    #[TODO]#
    #procss the element, using at most the 1000 dimensions of memory
    #return True if the element is determined to be in the bloom filter set
    #result = True if random() < .005 else False
    
    # bloom_hash_len = memory1b[4]# number of hash functions
    # bloom_hash_start = 5 #starting index of hash functions
    # bloom_buckets_len = memory1b[bloom_hash_start+bloom_hash_len] #number of buckets in bit array
    # bloom_buckets_start = bloom_hash_start+bloom_hash_len+1 #start of buckets
    bloom_buckets_len = 160000
    bloom_hash_len = 8
    bloom_buckets_start = 0

    element = element[1:-1] # pull off parenthesis from string
    element = element.split(", ") #split on commas to make array of rgb values
    
    #divide by 100 to allow values to be rounded to the nearest 100 allows for abs(r1 - r2) < 100

    # for r in range(-1, 2):
    #     for g in range(-1 ,2 ):
    #         for b in range(-1, 2):
    #             we_good = True
    #             total = str(int(element[0])+r)+","+str(int(element[1])+g)+","+str(int(element[2])+b)
    #             # for seed in range(bloom_hash_start, bloom_hash_start+bloom_hash_len):
    #             for seed in range(bloom_hash_len):
    #                 # pos = mmh3.hash(total, memory1b[seed]) % bloom_buckets_len
    #                 pos = mmh3.hash(total, seed) % bloom_buckets_len
    #                 if(memory1b[bloom_buckets_start+pos]==False):
    #                     we_good = False
    #                     break
    #             if we_good:
    #                 return True        
    r_val = ""
    g_val = ""
    b_val = ""
    for seed in range(bloom_hash_len):
        if(seed % 4 == 0):
            pos1 = mmh3.hash(element[0], seed) % bloom_buckets_len 
            pos2 = mmh3.hash(str(int(element[0])-1), seed) % bloom_buckets_len 
            pos3 = mmh3.hash(str(int(element[0])+1), seed) % bloom_buckets_len 
            if(not (memory1b[bloom_buckets_start+pos1] or memory1b[bloom_buckets_start+pos2] or memory1b[bloom_buckets_start+pos3])):
                return False
            else:
                if( memory1b[bloom_buckets_start+pos1]==True):
                    r_val = element[0]
                if( memory1b[bloom_buckets_start+pos2]==True):
                    r_val = str(int(element[0])-1)
                if( memory1b[bloom_buckets_start+pos3]==True):
                    r_val = str(int(element[0])+1)
        elif(seed % 4 == 1):
            pos1 = mmh3.hash(element[1], seed) % bloom_buckets_len 
            pos2 = mmh3.hash(str(int(element[1])-1), seed) % bloom_buckets_len 
            pos3 = mmh3.hash(str(int(element[1])+1), seed) % bloom_buckets_len 
            if(not (memory1b[bloom_buckets_start+pos1] or memory1b[bloom_buckets_start+pos2] or memory1b[bloom_buckets_start+pos3])):
                return False
            else:
                if( memory1b[bloom_buckets_start+pos1]==True):
                    g_val = element[1]
                if( memory1b[bloom_buckets_start+pos2]==True):
                    g_val = str(int(element[1])-1)
                if( memory1b[bloom_buckets_start+pos3]==True):
                    g_val = str(int(element[1])+1)
        elif(seed % 4 == 2):
            pos1 = mmh3.hash(element[2], seed) % bloom_buckets_len 
            pos2 = mmh3.hash(str(int(element[2])-1), seed) % bloom_buckets_len 
            pos3 = mmh3.hash(str(int(element[2])+1), seed) % bloom_buckets_len 
            if(not (memory1b[bloom_buckets_start+pos1] or memory1b[bloom_buckets_start+pos2] or memory1b[bloom_buckets_start+pos3])):
                return False
            else:
                if( memory1b[bloom_buckets_start+pos1]==True):
                    b_val = element[2]
                if( memory1b[bloom_buckets_start+pos2]==True):
                    b_val = str(int(element[2])-1)
                if( memory1b[bloom_buckets_start+pos3]==True):
                    b_val = str(int(element[2])+1)
        # elif(seed % 5 == 3):
        #     total = (int(element[0])) + (int(element[1])) + (int(element[2]))
        #     pos1 = mmh3.hash(str(total), seed) % bloom_buckets_len 
        #     pos2 = mmh3.hash(str(total+1), seed) % bloom_buckets_len 
        #     pos3 = mmh3.hash(str(total+2), seed) % bloom_buckets_len 
        #     pos4 = mmh3.hash(str(total+3), seed) % bloom_buckets_len 
        #     pos5 = mmh3.hash(str(total-1), seed) % bloom_buckets_len 
        #     pos6 = mmh3.hash(str(total-2), seed) % bloom_buckets_len 
        #     pos7 = mmh3.hash(str(total-3), seed) % bloom_buckets_len 
        #     if(not (memory1b[bloom_buckets_start+pos1] or 
        #             memory1b[bloom_buckets_start+pos2] or 
        #             memory1b[bloom_buckets_start+pos3] or 
        #             memory1b[bloom_buckets_start+pos4] or 
        #             memory1b[bloom_buckets_start+pos5] or 
        #             memory1b[bloom_buckets_start+pos6] or 
        #             memory1b[bloom_buckets_start+pos7])):
        #         return False
        elif(seed %4 == 3):
            pos = mmh3.hash(r_val+","+g_val+","+b_val,seed)%bloom_buckets_len
            if(memory1b[bloom_buckets_start+pos]==False):
                return False
            

    return True

    #replace the following line with the result
    # return result

##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([5**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    all = []#DEBUG
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = line.strip()
            #all.append(element)#DEBUG
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1A_meanRGBsStream(element, returnResult=True)
                print(" Result at stream element # %d:" % i)
                print("   1A:   means: %s" % str(["%.2f" % float(m) for m in result1a]))
                print(" [current memory size: %d]\n" % \
                    (getMemorySize(memory1a)))
                
            else: #just pass for stream processing
                result1a = task1A_meanRGBsStream(element, False)
                
            try:
                memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if memUsage > peakMem: peakMem = memUsage
            except:
                pass
        
    print("\n*******************************\n    Stream mean Terminated \n*******************************")
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")

    peakMem = 0 #tracks peak memory usage
    bloomSetSize = min(2000,0.1*i) #set the bloom filter set size (smaller for trial data)
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        bloomSet = []
        for line in infile:
            bloomSet.append(line.strip())
            #all.append(element)#DEBUG
            i += 1
            if i > bloomSetSize:
                break
        #setup bloom
        task1B_bloomSetup(bloomSet)        

        print("\n*******************************\n   Bloom Setup, Streaming: \n*******************************")
        
        # true_count = 0
        for line in infile:
            #remove \n and convert to int
            element = line.strip()
            i += 1
            #call tasks
            result1b = task1B_bloomStream(element)
            if result1b: #print status at this point:                
                print(" Result at stream element # %d:" % i)
                print("   1B: element: %s" % str(element))
                print("   1B:   bloom: %s" % str(result1b))
                print(" [current memory size: %d]\n" % \
                    (getMemorySize(memory1b)))              
                # true_count+=1
            try:
                memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if memUsage > peakMem: peakMem = memUsage
            except:
                pass
    
    print("\n*******************************\n   Stream bloom Terminated \n*******************************")
    # print("true count: %s" % str(true_count))
    # print(memory1b)
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")

#(250, 0, 0)        
#(100, 75, 50)
#(100, 100, 100)
#(100, 100, 50)
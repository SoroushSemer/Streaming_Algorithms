##########################################################################
## CSE545sp23_streamingV2_lastname_id.py
## version 2:
##   -- prints stream outputs in more helpful format
##   -- adds element=line.strip() in task1B
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name:
## Student ID: 


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
memory =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1A_meanRGBsStream(element, returnResult = True):
    #[TODO]#
    #procss the element you may only use memory, storing at most 1000
    
    if returnResult: #when the stream is requesting the current result
        result = (0.0, 0.0, 0.0)
        #[TODO]#
        #any additional processing to return the result at this point
        return result
    else: #no need to return a result
        pass


def task1B_bloomSetup(elements_in_set):
    #[TODO]#
    #setup the bloom filter memory to be able to filter streaming elements
    return 
    
def task1B_bloomStream(element):
    #[TODO]#
    #procss the element, using at most the 1000 dimensions of memory
    #return True if the element is determined to be in the bloom filter set
    result = True if random() < .005 else False
    
    #replace the following line with the result
    return result

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
                    (getMemorySize(memory)))
                
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
                    (getMemorySize(memory)))              
            try:
                memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if memUsage > peakMem: peakMem = memUsage
            except:
                pass
        
    print("\n*******************************\n   Stream bloom Terminated \n*******************************")
    if peakMem > 0:
        print("(peak memory usage was: ", peakMem, ")")

        

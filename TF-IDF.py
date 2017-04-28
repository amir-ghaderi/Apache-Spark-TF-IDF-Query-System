from pyspark import SparkConf, SparkContext
# The Math Module is imported for the log function
import math 


#Input: Directory of textfiles 
#Output: Directory of textfiles, where each word contains a assoicated TF-IDF value

def main(sc):
    #Iterates through all 265 text files located on HDFS
    #The reason for the multiple if statments is merely for the different textfile names
    for i in range(1,266): 
        if i==1:
            #Convertes i into a string so it can be concatenated into a file name
            i = str(i)
            #String concatenation
            file_location = "/user/amir/football/00" + i + ".txt"
            #Loads RDD
            text = sc.textFile(file_location)
            #Splits on spaces
            words = text.flatMap(lambda line: line.split())
            #Creates word counter
            wordWithCount = words.map(lambda word: (word, 1))
            #Basic word count
            final_rdd = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)  
        elif i > 1 and i < 10:
            i = str(i)
            file_location = "/user/amir/football/00" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            #Union of the first RDD with all other RDDs, note RDD union doesn't remove duplicates 
            final_rdd = sc.union([final_rdd,count])
        elif i  >=10 and i <100:
            i = str(i)
            file_location = "/user/amir/football/0" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            final_rdd = sc.union([final_rdd,count])
        elif i >=100:
            i = str(i)
            file_location = "/user/amir/football/" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            final_rdd = sc.union([final_rdd,count])


#Part 2: Using the Master RDD to obtain the IDF for each word
    #Reset the count of each word in the Master RDD
    wordWithCount = final_rdd.map(lambda x:(x[0],1))
    #Word Count on the reset Master RDD
    count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
    #Apply the IDF formula to all values of the Master RDD
    final_rdd2 = count.map(lambda x:(x[0],round(math.log(float(265)/float(x[1])),5)))


#Part 3: Calculate TF-IDF for all unquie words in each textfile

    for i in range(1,266):
        if i < 10:
            i = str(i)
            file_location = "/user/amir/football/00" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            #Union with the file containing the IDF scores
            merge = sc.union([count,final_rdd2])
            #Reduce by Key using multiplication
            count2 = merge.reduceByKey(lambda v1,v2: v1*v2)
            #Saving the output as textfile
            count2.saveAsTextFile("/user/amir/final/output/part" + i)
        elif i  >=10 and i <100:
            i = str(i)
            file_location = "/user/amir/football/0" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            merge = sc.union([count,final_rdd2])
            count2 = merge.reduceByKey(lambda v1,v2: v1*v2)
            count2.saveAsTextFile("/user/amir/final/output/part" + i)
        elif i >=100:
            i = str(i)
            file_location = "/user/amir/football/" + i + ".txt"
            text = sc.textFile(file_location)
            words = text.flatMap(lambda line: line.split())
            wordWithCount = words.map(lambda word: (word, 1))
            count = wordWithCount.reduceByKey(lambda v1,v2: v1+v2)
            merge = sc.union([count,final_rdd2])
            count2 = merge.reduceByKey(lambda v1,v2: v1*v2)
            count2.saveAsTextFile("/user/amir/final/output/part" + i)

    

    

        

if __name__  == "__main__":
    conf = SparkConf().setAppName("Creating TF-IDF")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()

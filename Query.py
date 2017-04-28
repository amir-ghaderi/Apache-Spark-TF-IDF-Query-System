from pyspark import SparkConf, SparkContext
import operator #Used to sort the dictionary

#Input: query = string and n = int
#Output: Top n document names and their respective TF-IDF scores in a textfile

def main(sc):
    #Number of documents you wish to retrieve
    n =10
    #The query you wish to search
    query = "What an amazing goal"
    #Splitting the query into a list of strings
    query = query.split()
    #Storing the length of the query
    length = len(query)
    #Initializing an empty dictionary
    score_board = {}
    #Iterating through all files and computing a total score per file
    for i in range(1,266):
        #initializing a score for the current file
        score = []
        #initializing the total number of matches for the current file
        matches = 0
        #initializing a score total
        total = 0
        #Converting i to a string
        i = str(i)
        #Grab the textfile
        file_location = "/user/amir/final/output/part" + i
        #Convert the textfile into a RDD
        text = sc.textFile(file_location)
        #Iterate through each word in the query you are using
        for j in query:
            #Check if the file contains the word
            count = text.filter(lambda x: j in x)
            if count.count() >= 1:
                #Increment matches
                matches = matches + 1
                #Retrieve the score
                word_score = count.map(lambda x: x[-19:-10])
                #Turn the score into a float
                float_score = word_score.map(lambda x: float(x))
                #Append the score to a list
                score.append(float_score.take(1))

        #Iterates through all scores in the list and aggregates them
        for p in score:
            total = total + p[0]
        #Multiple the summed score by the fraction of matches/length
        total = total * float(matches)/float(length)
        
        #Append the document name (key) and final score (value) to dictionary
        score_board["Document Number = " + i] = total

    #Sort dictionary
    final_score_board = sorted(score_board.items(), key=operator.itemgetter(1))
    #Reverse the sort
    final_score_board.reverse()
    #Grab the first n documents
    results = final_score_board[0:n]
    #Turn the object into a RDD
    result = sc.parallelize(results)
    #Save the output as a textfile
    result.saveAsTextFile("/user/amir/final/answer")

    
            
        


        
        








if __name__  == "__main__":
    conf = SparkConf().setAppName("Query")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()

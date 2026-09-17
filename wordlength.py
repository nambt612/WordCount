from pyspark import SparkConf, SparkContext 
import re 
import sys 
 
conf = SparkConf().setAppName("WordLength") 
sc = SparkContext(conf=conf) 
 
input_path = sys.argv[1] 
output_path = sys.argv[2] 
 
lines = sc.textFile(input_path) 
words = lines.flatMap(lambda line: line.split()) 
 
def classify(word): 
    word = re.sub(r'[^a-zA-Z0-9À-ỹ]', '', word) 
    if not word: 
        return None 
    length = len(word) 
    if length == 1: 
        category = "Rat nho" 
    elif 2 <= length <= 4: 
        category = "Nho" 
    elif 5 <= length <= 9: 
        category = "Trung binh" 
    else: 
        category = "Lon" 
    return (category, 1) 
 
result = words.map(classify).filter(lambda x: x is not None).reduceByKey(lambda a, b: a + b) 
result.saveAsTextFile(output_path) 
 
print("===== WORD LENGTH RESULT =====") 
for category, count in result.collect(): 
    print(category, count) 
 
sc.stop() 

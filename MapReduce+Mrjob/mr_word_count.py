from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import string
regex=re.compile(r"[\w]+")
class count_words(MRJob):
    def mapper(self,_,lines):
        for word in re.findall(regex,lines):
            word=word.strip(string.punctuation)
            word=word.lower()
            if word: yield (word,1)
    def combiner(self,word,counts):
        yield (word, sum(counts))
    def reducer(self,word,counts):
        yield (word,sum(counts))
            
if __name__=="__main__":
    count_words.run()

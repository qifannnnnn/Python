from mrjob.job import MRJob
from mrjob.step import MRStep
import re
regex=re.compile(r"-*\d+[.]{0,1}\d*")
class summary_stats(MRJob):
    def steps(self):
        return [ MRStep(mapper=self.mapper,combiner=self.combiner,reducer=self.reducer),
             MRStep(reducer=self.reducer2)]
    def mapper(self,_,line):
        i,j=re.findall(regex,line)
        yield int(i), (float(j),float(j)**2, 1)
    def combiner(self,key,v):
        sums,squares,counts=0,0,0
        for i in v:
            sums+=i[0]
            squares+=i[1]
            counts+=i[2]
        yield key,(sums,squares,counts)
    def reducer(self,key,v):
        sums,squares,counts=0,0,0
        for i in v:
            sums+=i[0]
            squares+=i[1]
            counts+=i[2]
        yield key,(sums,squares,counts)
    def reducer2(self,key,v):
        v=tuple(v)
        v=v[0]
        yield key,(v[2],v[0]/v[2],v[1]/v[2]-(v[0]/v[2])**2)

if __name__=="__main__":
    summary_stats.run()

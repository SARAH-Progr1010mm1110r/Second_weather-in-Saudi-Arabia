
from mrjob.job import MRJob
from mrjob.step import MRStep

class hadoop(MRJob):
    def steps(self):
        return[
            MRStep(
            mapper=self.mapper_names,
                reducer=self.reducer_names
            )
            ,
                        MRStep(
            mapper=self.mapper_names2,
                reducer=self.reducer_names2
            )
        ]
    def mapper_names(self,_,line):
        (YEAR,station_name,observation_date,elevation,wind_direction_angle,wind_type,wind_speed_rate,sky_ceiling_height,
         sky_cavok,visibility_distance,air_temperature,GEOPOINT) = line.split(',')
        yield ((station_name,air_temperature),1)
        
    def reducer_names (self,keys,values):
        yield (keys,sum(values))
        
    def mapper_names2(self,keys,values):
        (station_name,air_temperature) = keys
        yield (station_name,(air_temperature,values))
        
    def reducer_names2 (self,key2,values2):
        yield (key2,max(values2, key=lambda x:x[1]))   
    
    
        
if __name__ == "__main__":
    hadoop.run()

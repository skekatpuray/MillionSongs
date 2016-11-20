from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import h5py
import os.path


H5_BASEDIR = 'data'

list1 = []

_conf = (SparkConf().setMaster("local").setAppName("Million Songs"))
sc = SparkContext(conf = _conf)
sqlContext = SQLContext(sc)

nanRecords = sc.accumulator(0)

def readContents(filename):
    with  h5py.File(filename,'r') as hf:
        metadataGroup = hf.get('metadata')
        analysisGroup = hf.get('analysis')
        msongs = metadataGroup.get('songs')
        asongs = analysisGroup.get('songs')
        item = (
            msongs['artist_name'][0] if msongs['artist_name'] else None,#0  
            msongs['artist_id'][0] if msongs['artist_id'] else None,#1
            msongs['artist_familiarity'][0] if msongs['artist_familiarity'] else None,#2
            msongs['artist_name'][0] if msongs['artist_name'] else None,#3
            msongs['release'][0] if msongs['release'] else None,#4
            str(msongs['artist_mbid'][0]) if msongs['artist_mbid'] else None,#5
            msongs['artist_hotttnesss'][0] if msongs['artist_hotttnesss'] else None,#6
            str(msongs['artist_playmeid'][0]) if msongs['artist_playmeid'] else None,#7
            msongs['artist_latitude'][0] if msongs['artist_latitude'] else None ,#8
            msongs['artist_longitude'][0] if msongs['artist_longitude'] else None,#9
            str(msongs['artist_location'][0]) if msongs['artist_location'] else None,#10
            str(msongs['release_7digitalid'][0]) if msongs['release_7digitalid'] else None,#11
            msongs['song_id'][0] if msongs['song_id'] else None,#12
            msongs['song_hotttnesss'][0] if msongs['song_hotttnesss'] else None,#13
            str(msongs['title'][0]) if msongs['title'] else None,#14
            msongs['track_7digitalid'][0] if msongs['track_7digitalid'] else None,#15
            asongs['analysis_sample_rate'][0] if asongs['analysis_sample_rate'] else None,#16
            asongs['audio_md5'][0] if asongs['audio_md5'] else None,#17
            asongs['danceability'][0] if asongs['danceability'] else None,#18
            asongs['duration'][0] if asongs['duration'] else None,#19
            asongs['end_of_fade_in'][0] if asongs['end_of_fade_in'] else None,#20
            asongs['energy'][0] if asongs['energy'] else None,#21
            asongs['key'][0] if asongs['key'] else None,#22
            asongs['key_confidence'][0] if asongs['key_confidence'] else None,#23
            asongs['loudness'][0] if asongs['loudness'] else None,#24
            asongs['mode_confidence'][0] if asongs['mode_confidence'] else None,#25
            asongs['start_of_fade_out'][0] if asongs['start_of_fade_out'] else None,#26
            asongs['tempo'][0] if asongs['tempo'] else None,#27
            asongs['time_signature'][0] if asongs['time_signature'] else None,#28
            asongs['time_signature_confidence'][0] if asongs['time_signature_confidence'] else None#29
            )
        list1.append(item)
    return

#----------------------------------------------------------
#Assumption: Either a folder has a subfolder or an .h5 file
#----------------------------------------------------------
def visitFolder(param):
    for filename in os.listdir(param):
        if (os.path.isfile(param + '/' + filename) == False):
                visitFolder(param + '/' + filename)
        else:
            readContents(param + '/' + filename)
    return


visitFolder(H5_BASEDIR)

#Convert list of tuples to RDD
#-----------------------------
rdd = sc.parallelize(list1)


#Count records with bad lat/longitude features. 
#----------------------------------------------
for rec in rdd.collect():
    if (math.isnan(rec[8])):
            nanRecords += 1

#Save complete data-set.
#------------------------
rdd.saveAsTextFile('MillionSongs/AllFeatures')



#----------------------------------
# D A T A  C L E A N U P
#----------------------------------
#Picking up only measurement/categories based features. 
#Excluding all identifier based features.  
#Excluding features having majority observations having None/NaN.
#-----------------------------------------------------


#Remove rows having None for artist hotness since we are considering 
#this attribute as prediction value, and the rest as feature-set.
features1 = rdd.filter(lambda rec: rec[6] != None)


featuresRDD = features1.map(lambda item: (item[6],int(item[7]),item[16],item[19],item[20],item[22],item[23],item[24],item[25],item[26],item[27],item[28],item[29]))
featuresRDD.saveAsTextFile('MillionSongs/f2')

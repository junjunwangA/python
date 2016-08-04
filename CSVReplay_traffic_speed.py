import time
import datetime
import random
import psycopg2
from optparse import OptionParser
import StringIO

def parse_options():
    parser = OptionParser()
    parser.add_option("-f",    "--file-name", type="string", dest="data_file",    default='Data/Live_RWIS_Data_-_Traffic_Speeds2.csv', help="data file")
    parser.add_option("-s", "--stream-name", type="string", dest="dest_stream_fullname", default='weather.site_trafficspeed', help="stream name")
    parser.add_option("-o", "--send-offset", type="float", dest="offest", default='0.3', help="simulator send frequency")
    return parser.parse_args()

def main(options):
    dest_stream_fullname=options.dest_stream_fullname
    data_file=options.data_file
    offest=options.offest

#     conn = psycopg2.connect("host=10.79.57.177  dbname=cqdb user=primea")
    conn = psycopg2.connect("dbname=cqdb user=primea")
    cur = conn.cursor()
    # push data                                                                                     
    while True:
        fo = open(data_file,'r')
        flag=0
        for line in fo.readlines():
            try:
                time.sleep(offest+random.random())
            except IOError as ierr:
                print "invalid offset in handler " + str(offest)
            if flag==0:
                flag=-1
                continue
            latitude=line.split(',')[11]
            longitude=line.split(',')[12]
            timeA=line.split(',')[4]     
            line=line.replace(latitude,latitude[2:].strip())
            line=line.replace(longitude,longitude[:-3].strip())
            line=line.replace(timeA,time.ctime())
#             print "line:%s"%(line)
            cur.copy_from(StringIO.StringIO(line), dest_stream_fullname, sep=',')
        fo.close()
#         cur.close()
#         conn.commit()           
        flag = 0       

if __name__ == '__main__':
    options, args = parse_options()
    main(options)

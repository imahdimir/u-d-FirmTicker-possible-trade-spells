"""

  """

##

import sys

import pyspark.sql.functions as sfunc
from mirutil.df_utils import save_as_prq_wo_index as sprq
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import concat_ws

from shared import _ret_df_of_every_second_in_day
from shared import _ret_status_change_data
from shared import _ret_tsetmc_id_data
from shared import _ret_working_date_data
from shared import c


spark = SparkSession.builder.getOrCreate()

def main() :

  pass

  ##
  dst = _ret_status_change_data()
  dtw = _ret_working_date_data()
  did = _ret_tsetmc_id_data()
  ##
  dti = _ret_df_of_every_second_in_day()
  ##
  sdt = spark.createDataFrame(dti)
  sdw = spark.createDataFrame(dtw)
  ##
  for tid in did[c.id] :
    print(tid)

    did1 = did[did[c.id].eq(tid)]

    sdi = spark.createDataFrame(did1)

    sd = sdi.crossJoin(sdw)
    sd = sd.crossJoin(sdt)

    newcol = concat_ws(' ' , sd.Date , sd.Time).alias(c.dt)
    sd = sd.select(c.id , newcol , c.ismktopen)

    sds = spark.createDataFrame(dst)

    sd = sd.join(sds , [c.id , c.dt] , how = 'outer')

    sd = sd.sort([c.id , c.dt])

    window = Window.partitionBy(c.id).orderBy(c.dt).rowsBetween(-sys.maxsize ,
                                                                0)
    filled_column = sfunc.last(sd[c.trdble] , ignorenulls = True).over(window)
    sdf_filled = sd.withColumn('filled' , filled_column)

    sd = sdf_filled

    msk = sd['filled'] == True
    msk &= sd[c.ismktopen] == True
    sd = sd.filter(msk)

    sd = sd.select(c.id , c.dt)

    sd = sd.withColumn(c.d , sfunc.substring(c.dt , 1 , 10))

    sd = sd.groupBy([c.id , c.d]).count()

    sdv = sd.toPandas()

    sprq(sdv , f'{tid}.prq')

    break

  ##


  ##


  ##


  ##


  ##


  ##


  ##


  ##

##


##


if __name__ == '__main__' :
  main()
  print('done')

##
import dulwich


##
url = 'https://github.com/imahdimir/d-clean-d-firm-status-change'
dulwich.porcelain.clone(url)

##


##
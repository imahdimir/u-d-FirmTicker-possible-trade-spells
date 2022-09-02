"""

  """

##

from datetime import time
from datetime import datetime as dt

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class RepoAddresses :
  tids = 'imahdimir/d-TSETMC_ID-2-Ticker'
  wds = 'imahdimir/d-TSE-working-days'

ra = RepoAddresses()

class Constants :
  mkt_start_time = time(9 , 00)
  mkt_end_time = time(12 , 30)

cte = Constants()

class ColumnNames :
  tid = 'TSETMC_ID'
  jd = 'JDate'
  d = 'Date'
  st = 'StartTime'
  et = 'EndTime'
  dur = 'Duration'
  sdt = 'StartDateTime'
  edt = 'EndDateTime'
  sjdt = 'StartJDateTime'
  ejdt = 'EndJDateTime'

c = ColumnNames()

def main() :

  pass

  ##
  rp_tid = GithubData(ra.tids)
  df_tid = rp_tid.read_data()
  ##
  df_tid.reset_index(inplace = True)
  ##
  df_tid = df_tid[[c.tid]]
  ##

  rp_wds = GithubData(ra.wds)
  df_wds = rp_wds.read_data()
  ##
  df_wds.reset_index(inplace = True)
  ##
  df_wds = df_wds[[c.jd , c.d]]
  ##
  for cn , val in zip([c.st , c.et] , [cte.mkt_start_time , cte.mkt_end_time]) :
    df_wds[cn] = val
  ##
  _adc = {
      c.sjdt : lambda x : str(x[c.jd]) + ' ' + str(x[c.st]) ,
      c.ejdt : lambda x : str(x[c.jd]) + ' ' + str(x[c.et]) ,
      c.sdt  : lambda x : str(x[c.d]) + ' ' + str(x[c.st]) ,
      c.edt  : lambda x : str(x[c.d]) + ' ' + str(x[c.et]) ,
      }

  for ky , val in _adc.items() :
    df_wds[ky] = df_wds.apply(val , axis = 1)

  ##
  df = pd.merge(df_tid , df_wds , how = 'cross')

  ##
  sprq(df , 'temp.prq')


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

##
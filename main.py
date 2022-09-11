"""

  """

import json
from datetime import time

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    ftic = gj['src0']
    wds = gj['src1']
    trg = gj['trg']

gu = GDUrl()

class Constants :
    mkt_start_time = time(9 , 00)
    mkt_end_time = time(12 , 30)

cte = Constants()

class ColumnNames :
    jd = 'JDate'
    st = 'StartTime'
    et = 'EndTime'
    sjdt = 'StartJDateTime'
    ejdt = 'EndJDateTime'
    ftic = 'FirmTicker'

c = ColumnNames()

def main() :
    pass

    ##

    gd_ftic = GithubData(gu.ftic)
    df = gd_ftic.read_data()
    ##

    gd_wds = GithubData(gu.wds)
    df_wds = gd_wds.read_data()
    ##
    df_wds = df_wds[[c.jd]]
    ##
    _zi = zip([c.st , c.et] , [cte.mkt_start_time , cte.mkt_end_time])
    for cn , val in _zi :
        df_wds[cn] = val

    ##
    _adc = {
            c.sjdt : lambda x : str(x[c.jd]) + ' ' + str(x[c.st]) ,
            c.ejdt : lambda x : str(x[c.jd]) + ' ' + str(x[c.et]) ,
            }

    for ky , val in _adc.items() :
        df_wds[ky] = df_wds.apply(val , axis = 1)

    ##
    do = pd.merge(df , df_wds , how = 'cross')
    ##
    do = do[[c.ftic , c.sjdt , c.ejdt]]
    dov = do.head()

    ##

    gdt = GithubData(gu.trg)
    gdt.overwriting_clone()
    ##
    dft = gdt.read_data()

    ##
    dft = pd.concat([dft , do])
    ##
    dft = dft.drop_duplicates()
    ##

    fp = gdt.data_fp
    sprq(dft , fp)

    ##
    msg = 'updated by'
    msg += ' ' + gu.cur
    ##

    gdt.commit_and_push(msg)

    ##


    for rp in [gd_ftic , gd_wds , gdt] :
        rp.rmdir()


    ##

##
if __name__ == '__main__' :
    main()
    print('done')

##

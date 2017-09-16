
import pymysql
import pandas as pd

db=pymysql.connect(host="",    # your host, usually localhost
                         user="",         # your username
                         passwd="",  # your password
                         db="",
                         charset="utf8")
cur=db.cursor()



def MakeColRel(x,y,z):
    query="select a.edl_idx, b.etl_idx, b.ecl_idx, a.eil_idx \
    from chu_indicator_list a inner join chu_col_list_ref b on a.etl_idx=b.etl_idx \
    where b.ecl_year in (%s) and b.ecl_eng_name in (%s) \
    and a.eil_kor_name=%s"
    cur.execute(query,(x,y,z))
    print(list(cur.fetchall()))


def ImpColRel(x,y,z):
    query="select edl_idx,etl_idx,ecl_idx,eil_idx \
    from chu_indicator_column_relation where ecl_idx in \
    (select ecl_idx from chu_col_list_ref where ecl_year in (%s)) \
    and ecl_idx in (select ecl_idx from chu_col_list_ref where ecl_eng_name in (%s)) \
    and eil_idx in (select eil_idx from chu_indicator_list where eil_kor_name=%s)"
    cur.execute(query,(x,y,z))
    print(list(cur.fetchall()))


class CheckColRel:
    def __init__(self,ecl_year,edl_eng_name,eil_kor_name):
        self.ecl_year=ecl_year
        self.edl_eng_name=edl_eng_name
        self.eil_kor_name=eil_kor_name

    def LetsCheck(x,y,z):
        if MakeColRel(x,y,z) == ImpColRel(x,y,z):
            print("No Problem")
        else :
            print("it's different")


CheckColRel.LetsCheck(2011,'bd1','월간음주율')



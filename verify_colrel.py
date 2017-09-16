
import pymysql
import pandas as pd

db=pymysql.connect(host="",    # your host, usually localhost
                         user="",         # your username
                         passwd="",  # your password
                         db="",
                         charset="utf8")
cur=db.cursor()


class CheckColRel:
    def __init__(self,x,y,z):
        self.x=
        self.y=y
        self.z=z
        
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


def LetsCheck(x,y,z):
    a=CheckColRel.MakeColRel(x,y,z)
    b=CheckColRel.ImpColRel(x,y,z)
    print(a)
    print(b)
    if a == b:
        print("Perfect!")
    else:
        commonset=set(b)-set(a)
        print("There's difference.")
        print("Change the medata data:",commonset)

LetsCheck(('2008','2009','2010','2011','2012','2013'),('p','hhid'),'가계직접부담 의료비')



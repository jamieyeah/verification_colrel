
import pymysql

db=pymysql.connect(host="",    
                         user="",         
                         passwd="",  
                         db="",
                         charset="utf8")
cur=db.cursor()


class CheckColRel:
    def __init__(self,x,y,z):
        self.x=x
        self.y=y
        self.z=z
        
    def MakeColRel(x,y,z):
        query="select a.edl_idx, b.etl_idx, b.ecl_idx, a.eil_idx \
        from chu_indicator_list a inner join chu_col_list_ref b on a.etl_idx=b.etl_idx \
        where b.ecl_year =%s and b.ecl_eng_name = %s \
        and a.eil_kor_name=%s \
        order by b.ecl_idx"
        cur.execute(query,(x,y,z))
        print(list(cur.fetchall()))


    def ImpColRel(x,y,z):
        query="select edl_idx,etl_idx,ecl_idx,eil_idx \
        from chu_indicator_column_relation where ecl_idx in \
        (select ecl_idx from chu_col_list_ref where ecl_year =%s) \
        and ecl_idx in (select ecl_idx from chu_col_list_ref where ecl_eng_name =%s) \
        and eil_idx in (select eil_idx from chu_indicator_list where eil_kor_name=%s) \
        order by ecl_idx"
        cur.execute(query,(x,y,z))
        print(list(cur.fetchall()))
        
    def CheckSimple(z):
            query="select year, ecl_eng_name, eil_kor_name from \
            health_care_ui.chu_verification_indicator \
            where eil_kor_name=%s"
            cur.execute(query,z)
            return(list((cur.fetchall())))


def LetsCheckColRel(z):
    c=CheckColRel.CheckSimple(z)
    for x,y,z in c:
        for n in range(0,len(c)):
            print(n)
            x=c[n][0]
            y=c[n][1]
            z=c[n][2]
            NewRel=CheckColRel.MakeColRel(x,y,z)
            OldRel=CheckColRel.ImpColRel(x,y,z)
            print(NewRel)
            print(OldRel)
            if OldRel == NewRel:
                print("Perfect!")
            elif OldRel == []:
                print("You didn't insert the data!")
                print("The data is inserted into the medatabase!:", NewRel)
                for q1,x1,y1,z1 in NewRel:
                    query="insert into health_care_ui.chu_indicator_column_relation \
                    (eicr_idx, edl_idx, etl_idx, ecl_idx, eil_idx) values ('0',%s,%s,%s,%s)"
                    cur.execute(query,(q1,x1,y1,z1))
                    db.commit()
            else:
                print("There's a difference. It's going to be updated automatically!")
                print("The changed metadata:",OldRel, "into", NewRel)
                for q1,x1,y1,z1 in NewRel:
                    for q2,x2,y2,z2 in OldRel:
                        query="select eicr_idx from health_care_ui.chu_indicator_column_relation \
                        where edl_idx=%s and etl_idx=%s and ecl_idx=%s and eil_idx=%s"
                        cur.execute(query,(q2,x2,y2,z2))
                        w1=list(cur)
                        query2="update health_care_ui.chu_indicator_column_relation \
                        set edl_idx=%s, etl_idx=%s, ecl_idx=%s, eil_idx=%s \
                        where eicr_idx=%s"
                        cur.execute(query2,(q1,x1,y1,z1,w1))
                        db.commit()
        break

LetsCheckColRel('가계직접부담 의료비')



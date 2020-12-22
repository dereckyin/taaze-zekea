from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import json
from datetime import datetime, timedelta
import logging
import sys
import linecache
import os
import cx_Oracle
import uvicorn
import hashlib 
from enum import Enum

app = FastAPI()


User = os.getenv("MYUSER")
Pwd = os.getenv("MYPASSWD")

settingLog()

class Object:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    logging.error('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

def settingLog():
    # 設定
    datestr = datetime.today().strftime('%Y%m%d')
    if not os.path.exists("log/" + datestr):
        os.makedirs("log/" + datestr)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M:%S',
                        handlers = [logging.FileHandler('log/' + datestr + '/zekeapi.log', 'a', 'utf-8'),])

    
    # 定義 handler 輸出 sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # 設定輸出格式
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # handler 設定輸出格式
    console.setFormatter(formatter)
    # 加入 hander 到 root logger
    logging.getLogger('').addHandler(console)

def hashpassword(pwd):
    result = hashlib.md5(pwd.encode("ascii"))
    p1 = result.hexdigest().upper().replace("-", "")
    p2 = hashlib.md5(p1.encode("ascii"))
    res = p2.hexdigest().lower().replace("-", "")
    return res

def get_book_rank(rank):
    switcher = {
        'A': "全新品",
        'B': "近全新",
        'C': "良好",
        'D': "普通",
        'E': "差強人意",
    }
    return switcher.get(rank, "")

def get_video_url(logcode):
    url = ""
    if logcode:
        url = "https://vod.taaze.tw/vod/" + str(logcode) + ".mp4"
    return url

class UsedBook(BaseModel):
    prodid : str
    page : int
    size : int

@app.post('/getusedbookbyprodid')
async def getusedbookbyprodid(book: UsedBook, request: Request):
    _prod_id = book.prodid
    _page = book.page
    _pagesize = book.size
   
    if not _prod_id:
        raise HTTPException(status_code=401, detail="Missing parameter")

    userinfo = []
    
    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn(os.getenv("MYHOST"), '1521', None,os.getenv("MYDB")), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    sql = """SELECT res.*,
                CEIL(total_num_rows/%d) total_num_pages
            FROM  
            (
            SELECT T.PROD_ID,T.ORG_PROD_ID,T.TITLE_MAIN,T.PROD_RANK, 
            T.ADD_MARK_FLG, T.SALE_PRICE, T.LIST_PRICE, T.SALE_DISC, CUST_ID ,T.NICK_NAME,T.CUID,T.NOTE ,VIDEO.VIDEO_ID,VIDEO.USED_STATUS, WELFARE_ID, total_num_rows, rn FROM ( 
                SELECT PROD.PROD_ID,PROD.ORG_PROD_ID,PROD.TITLE_MAIN,PROD.PROD_RANK,
                PROD.ADD_MARK_FLG, PROD.SALE_PRICE, PROD.LIST_PRICE, PROD.SALE_DISC, CUS.CUST_ID ,CUS.NICK_NAME,CUS.CUID,PROD.NOTE,PROD.LOGCODE, COALESCE(D.WELFARE_ID, '') WELFARE_ID, row_number() OVER (ORDER BY PROD.prod_id DESC) rn,
                                    COUNT(*) OVER () total_num_rows
                FROM PRODUCT PROD, CUSTOMER CUS, VSTK V, SPROD_ASK_DETAIL D 
                WHERE PROD.PROD_ID=V.PROD_ID AND PROD.SUP_ID=CUS.CUST_ID AND D.PROD_ID=PROD.PROD_ID
                AND PROD.STATUS_FLG='Y' AND PROD.ORG_FLG='C' 
                AND V.QTY>0 AND PROD.ORG_PROD_ID = '%s') T 
            LEFT JOIN PRODINFO_VIDEO VIDEO ON T.LOGCODE=VIDEO.PROD_ID AND VIDEO.USED_STATUS <> 'N' ) res
                        WHERE rn BETWEEN (%d - 1) * %d + 1 AND %d * %d """
    cur.execute (sql % (_pagesize, _prod_id, _page, _pagesize, _page, _pagesize))

    try:
        row = cur.fetchone()
        while row:
            PROD_ID = row[0]
            ORG_PROD_ID = row[1]
            TITLE_MAIN = row[2]
            SALE_PRICE = row[5]
            LIST_PRICE = row[6]
            CUST_ID = row[8]
            CUID = row[10]
            NICKNAME = row[9]
            SALE_DISC = row[7]
            PROD_RANK = row[3]
            VIDEO_ID = row[12]
            WELFARE_ID = row[14]
            TOTAL_NUM_ROWS = row[15]
            TOTAL_NUM_PAGES = row[17]
            
            user = Object()
            user.PROD_ID = PROD_ID
            user.ORG_PROD_ID = ORG_PROD_ID

            user.SALE_PRICE = SALE_PRICE
            user.LIST_PRICE = LIST_PRICE
            user.TITLE_MAIN = TITLE_MAIN
            user.CUST_ID = CUST_ID
            user.CUID = CUID
            user.NICKNAME = NICKNAME
            user.SALE_DISC = SALE_DISC
            user.PROD_RANK = get_book_rank(PROD_RANK)
            user.VIDEO_URL = get_video_url(VIDEO_ID)
            user.WELFARE_ID = WELFARE_ID
            user.TOTAL_NUM_ROWS = TOTAL_NUM_ROWS
            user.TOTAL_NUM_PAGES = TOTAL_NUM_PAGES
            
            userinfo.append(user)
    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "getbookbycustid FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad getbookbycustid")
       
    finally:
        cur.close()
        con.close()

    return userinfo

class CustId(BaseModel):
    custid : str
    page : int
    size : int

@app.post('/getbookbycustid')
async def getbookbycustid(cuid: CustId, request: Request):
    _cust_id = cuid.custid
    _page = cuid.page
    _pagesize = cuid.size
   
    if not _cust_id:
        raise HTTPException(status_code=401, detail="Missing parameter")

    userinfo = []
    
    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn(os.getenv("MYHOST"), '1521', None,os.getenv("MYDB")), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    sql = """SELECT res.*,
                CEIL(total_num_rows/%d) total_num_pages
            FROM   (SELECT o.*, VIDEO.VIDEO_ID,
                        row_number() OVER (ORDER BY o.prod_id DESC) rn,
                        COUNT(*) OVER () total_num_rows
                    FROM   NEW_EC_SND_DTL o, PRODUCT PROD 
                    LEFT JOIN PRODINFO_VIDEO VIDEO ON PROD.LOGCODE=VIDEO.PROD_ID AND VIDEO.USED_STATUS <> 'N'
                    WHERE PROD.PROD_ID=o.PROD_ID and o.sup_id = '%s') res
            WHERE  rn BETWEEN (%d - 1) * %d + 1 AND %d * %d """
    cur.execute (sql % (_pagesize, _cust_id, _page, _pagesize, _page, _pagesize))

    try:
        row = cur.fetchone()
        while row:
            PROD_ID = row[0]
            ORG_PROD_ID = row[1]
            SALE_PRICE = row[4]
            LIST_PRICE = row[6]
            TITLE_MAIN = row[10]
            AUTHOR_MAIN = row[11]
            SALE_DISC = row[13]
            PROD_RANK = row[20]
            VIDEO_ID = row[22]
            TOTAL_NUM_ROWS = row[24]
            TOTAL_NUM_PAGES = row[25]
            
            user = Object()
            user.PROD_ID = PROD_ID
            user.ORG_PROD_ID = ORG_PROD_ID

            user.SALE_PRICE = SALE_PRICE
            user.LIST_PRICE = LIST_PRICE
            user.TITLE_MAIN = TITLE_MAIN
            user.AUTHOR_MAIN = AUTHOR_MAIN
            user.SALE_DISC = SALE_DISC
            user.PROD_RANK = get_book_rank(PROD_RANK)
            user.VIDEO_URL = get_video_url(VIDEO_ID)
            user.TOTAL_NUM_ROWS = TOTAL_NUM_ROWS
            user.TOTAL_NUM_PAGES = TOTAL_NUM_PAGES
            
            userinfo.append(user)
    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "getbookbycustid FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad getbookbycustid")
       
    finally:
        cur.close()
        con.close()

    return userinfo

class Login(BaseModel):
    email : str
    password : str

@app.post('/gettaazeuid')
async def gettaazeuid(param: Login, request: Request):
    _email = param.email
    _pwd = param.password
       
    if not _email:
        raise HTTPException(status_code=401, detail="Missing parameter")

    userinfo = []
    
    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn(os.getenv("MYHOST"), '1521', None,os.getenv("MYDB")), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    if not _pwd:
        sql = """select CUST_ID, CUID, substr(cust_id, 0, 2) from CUSTOMER where MAIL_MAIN = '%s' and substr(cust_id, 0, 2) in ('GL', 'FB') """
        cur.execute (sql % (_email))
    else:
        password = hashpassword(_pwd)
        # sql = """select CUST_ID, CUID, 'TZ' from CUSTOMER where MAIL_MAIN = '%s' and CUST_PW = '%s' """
        sql = """select CUST_ID, CUID, 'TZ' from CUSTOMER where CUST_ID = '%s' and CUST_PW = '%s' """
        cur.execute (sql % (_email, password))

    try:
        row = cur.fetchone()
        while row:
            CUST_ID = row[0]
            CUID = row[1]
            KIND = row[2]
            
            user = Object()
            user.CUST_ID = CUST_ID
            user.CUID = CUID
            user.KIND = KIND
            userinfo.append(user)
    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "gettaazeuid FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad gettaazeuid")
       
    finally:
        cur.close()
        con.close()

    return userinfo

class TAG(str, Enum):
    HOME_TRADITIONAL_B =  "HOME_TRADITIONAL_B"
    HOME_TRADITIONAL_PHILOSOPHY_RELIGION_B =  "HOME_TRADITIONAL_PHILOSOPHY_RELIGION_B"
    HOME_TRADITIONAL_COMPUTER_TECHNOLOGY_B =  "HOME_TRADITIONAL_COMPUTER_TECHNOLOGY_B"
    HOME_ENGLISH_EBOOK_B =  "HOME_ENGLISH_EBOOK_B"
    HOME_3HOURS_B =  "HOME_3HOURS_B"
    HOME_REMAINDER_DISC3_B =  "HOME_REMAINDER_DISC3_B"
    HOME_TRADITIONAL_CHINESE_LITERATURE_B =  "HOME_TRADITIONAL_CHINESE_LITERATURE_B"
    HOME_TRADITIONAL_COMIC_B =  "HOME_TRADITIONAL_COMIC_B"
    HOME_CREATIVE_DVD_B =  "HOME_CREATIVE_DVD_B"
    HOME_SIMPLIFIED_SCIENCE_NATURE_B =  "HOME_SIMPLIFIED_SCIENCE_NATURE_B"
    HOME_SIMPLIFIED_LITERATURE_B =  "HOME_SIMPLIFIED_LITERATURE_B"
    HOME_SIMPLIFIED_RELIGION_B =  "HOME_SIMPLIFIED_RELIGION_B"
    HOME_B =  "HOME_B"
    HOME_TRADITIONAL_ART_BOOKS_B =  "HOME_TRADITIONAL_ART_BOOKS_B"
    HOME_JAPANESE_MOOK_B =  "HOME_JAPANESE_MOOK_B"
    HOME_TRADITIONAL_HISTORY_GEOGRAPHY_B =  "HOME_TRADITIONAL_HISTORY_GEOGRAPHY_B"
    HOME_TRADITIONAL_SELF_IMPROVEMENT_B =  "HOME_TRADITIONAL_SELF_IMPROVEMENT_B"
    HOME_JAPANESE_EBOOK_B =  "HOME_JAPANESE_EBOOK_B"
    HOME_TRADITIONAL_PARENTING_FAMILIES_B =  "HOME_TRADITIONAL_PARENTING_FAMILIES_B"
    HOME_TRADITIONAL_LANGUAGE_B =  "HOME_TRADITIONAL_LANGUAGE_B"
    HOME_TRADITIONAL_SOCIAL_SCIENCES_B =  "HOME_TRADITIONAL_SOCIAL_SCIENCES_B"
    HOME_TRADITIONAL_EDUCATION_TEACHING_B =  "HOME_TRADITIONAL_EDUCATION_TEACHING_B"
    HOME_REMAINDER_DISC4_B =  "HOME_REMAINDER_DISC4_B"
    HOME_CHINESE_EBOOK_B =  "HOME_CHINESE_EBOOK_B"
    HOME_TRADITIONAL_OUTDOORS_ADVENTURE_B =  "HOME_TRADITIONAL_OUTDOORS_ADVENTURE_B"
    HOME_TRADITIONAL_GOVERNMENT_BOOKS_B =  "HOME_TRADITIONAL_GOVERNMENT_BOOKS_B"
    HOME_CREATIVE_GROCERIES_B =  "HOME_CREATIVE_GROCERIES_B"
    HOME_SIMPLIFIED_ART_BOOKS_B =  "HOME_SIMPLIFIED_ART_BOOKS_B"
    HOME_TRADITIONAL_FICTION_LITERATURE_B =  "HOME_TRADITIONAL_FICTION_LITERATURE_B"
    HOME_ENGLISH_MAGAZINE_B =  "HOME_ENGLISH_MAGAZINE_B"
    HOME_TRADITIONAL_SCIENCE_NATURE_B =  "HOME_TRADITIONAL_SCIENCE_NATURE_B"
    HOME_TRADITIONAL_LIFE_STYLE_B =  "HOME_TRADITIONAL_LIFE_STYLE_B"
    HOME_TRADITIONAL_MEDICAL_HEALTH_B =  "HOME_TRADITIONAL_MEDICAL_HEALTH_B"
    HOME_SIMPLIFIED_SOCIAL_SCIENCES_B =  "HOME_SIMPLIFIED_SOCIAL_SCIENCES_B"
    HOME_TRADITIONAL_NONFICTION_B =  "HOME_TRADITIONAL_NONFICTION_B"
    HOME_CHINESE_EMAGAZINE_B =  "HOME_CHINESE_EMAGAZINE_B"
    HOME_CHINESE_MAGAZINE_B =  "HOME_CHINESE_MAGAZINE_B"
    HOME_TRADITIONAL_ARCHITECTURE_DESIGN_B =  "HOME_TRADITIONAL_ARCHITECTURE_DESIGN_B"
    HOME_TRADITIONAL_BIOGRAPHY_B =  "HOME_TRADITIONAL_BIOGRAPHY_B"
    HOME_TRADITIONAL_BUSINESS_MONEY_B =  "HOME_TRADITIONAL_BUSINESS_MONEY_B"
    HOME_CREATIVE_STATIONERY_B =  "HOME_CREATIVE_STATIONERY_B"
    HOME_KOREA_MAGAZINE_B =  "HOME_KOREA_MAGAZINE_B"
    HOME_CREATIVE_CD_B =  "HOME_CREATIVE_CD_B"
    HOME_SIMPLIFIED_LIFE_B =  "HOME_SIMPLIFIED_LIFE_B"
    HOME_JAPANESE_MAGAZINE_B =  "HOME_JAPANESE_MAGAZINE_B"


class EditorChoice(BaseModel):
    kind : TAG

@app.post('/geteditorchoice')
async def geteditorchoice(param: EditorChoice, request: Request):
    _kind = param.kind
       
    if not _kind:
        raise HTTPException(status_code=401, detail="Missing parameter")

    userinfo = []
    
    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn(os.getenv("MYHOST"), '1521', None,os.getenv("MYDB")), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    sql = """select PM.HOMEPAGE_CODE, PM.PK_NO, PROD.PROD_ID,PM.PROD_CAT_ID,PM.CAT_ID,PROD.TITLE_MAIN,
            PROD.LIST_PRICE,PROD.SALE_DISC,PROD.SALE_PRICE,PROD.ORG_PROD_ID, 
            P.PUBLISH_DATE,P.AUTHOR_MAIN ,P.PUB_ID ,PS.PUB_NM_MAIN,NVL(EP.DOWNLOADS,0) ,TO_CHAR(NVL(EP.ORG_PROD_ID,'10000000000')),
            RANK() OVER(PARTITION BY PM.PROD_ID ORDER BY PM.PK_NO ) MM
            FROM PROD_PM_RECOMMEND PM ,PRODUCT PROD LEFT JOIN PRODINFO_MAIN P ON  PROD.ORG_PROD_ID=P.ORG_PROD_ID
            LEFT JOIN EPUB_PREVIEW EP ON EP.ORG_PROD_ID=P.ORG_PROD_ID AND EP.AP_CODE='taaze' 
            LEFT JOIN PUBLISHER PS ON P.PUB_ID=PS.PUB_ID 
            WHERE PM.PROD_ID=PROD.PROD_ID
            AND PM.HOMEPAGE_CODE = '%s'
            AND PROD.PUBLISH_DATE>=TO_CHAR(PM.CRT_TIME-365,'YYYYMMDD')
            AND rownum <= 50
            ORDER BY PM.STATUS_FLG ASC,PM.SEQ_ID ASC ,PROD.PUBLISH_DATE DESC NULLS LAST """
    cur.execute (sql % (_kind.value))

    try:
        row = cur.fetchone()
        while row:
            PROD_ID = row[2]
            ORG_PROD_ID = row[9]
            SALE_PRICE = row[8]
            LIST_PRICE = row[6]
            TITLE_MAIN = row[5]
            AUTHOR_MAIN = row[11]
            SALE_DISC = row[7]
            TOTAL_NUM_ROWS = 50
            TOTAL_NUM_PAGES = 1
            
            user = Object()
            user.PROD_ID = PROD_ID
            user.ORG_PROD_ID = ORG_PROD_ID

            user.SALE_PRICE = SALE_PRICE
            user.LIST_PRICE = LIST_PRICE
            user.TITLE_MAIN = TITLE_MAIN
            user.AUTHOR_MAIN = AUTHOR_MAIN
            user.SALE_DISC = SALE_DISC

            user.TOTAL_NUM_ROWS = TOTAL_NUM_ROWS
            user.TOTAL_NUM_PAGES = TOTAL_NUM_PAGES
            
            userinfo.append(user)
    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "geteditorchoice FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad geteditorchoice")
       
    finally:
        cur.close()
        con.close()

    return userinfo

class Welfare(BaseModel):
    welfare_id : str
    page : int
    size : int

@app.post('/getwelfarebook')
async def getwelfarebook(param: Welfare, request: Request):
    _welfare_id = param.welfare_id
    _page = param.page
    _pagesize = param.size
   
    if not _welfare_id:
        raise HTTPException(status_code=401, detail="Missing parameter")

    userinfo = []
    
    con = cx_Oracle.connect(User, Pwd, cx_Oracle.makedsn(os.getenv("MYHOST"), '1521', None,os.getenv("MYDB")), encoding="UTF-8", nencoding="UTF-8")
    cur = con.cursor()

    sql = """SELECT res.*,
                CEIL(total_num_rows/%d) total_num_pages
            FROM  
            (
            SELECT T.PROD_ID,T.ORG_PROD_ID,T.TITLE_MAIN,T.PROD_RANK, 
            T.ADD_MARK_FLG, T.SALE_PRICE, T.LIST_PRICE, T.SALE_DISC, CUST_ID ,T.NICK_NAME,T.CUID,T.NOTE ,VIDEO.VIDEO_ID,VIDEO.USED_STATUS, WELFARE_ID, total_num_rows, rn FROM ( 
                SELECT PROD.PROD_ID,PROD.ORG_PROD_ID,PROD.TITLE_MAIN,PROD.PROD_RANK,
                PROD.ADD_MARK_FLG, PROD.SALE_PRICE, PROD.LIST_PRICE, PROD.SALE_DISC, CUS.CUST_ID ,CUS.NICK_NAME,CUS.CUID,PROD.NOTE,PROD.LOGCODE, COALESCE(D.WELFARE_ID, '') WELFARE_ID, row_number() OVER (ORDER BY PROD.prod_id DESC) rn,
                                    COUNT(*) OVER () total_num_rows
                FROM PRODUCT PROD, CUSTOMER CUS, VSTK V, SPROD_ASK_DETAIL D 
                WHERE PROD.PROD_ID=V.PROD_ID AND PROD.SUP_ID=CUS.CUST_ID AND D.PROD_ID=PROD.PROD_ID
                AND PROD.STATUS_FLG='Y' AND PROD.ORG_FLG='C' 
                AND V.QTY>0 AND D.WELFARE_ID = '%s') T 
            LEFT JOIN PRODINFO_VIDEO VIDEO ON T.LOGCODE=VIDEO.PROD_ID AND VIDEO.USED_STATUS <> 'N' ) res
                        WHERE rn BETWEEN (%d - 1) * %d + 1 AND %d * %d """
    cur.execute (sql % (_pagesize, _welfare_id, _page, _pagesize, _page, _pagesize))

    try:
        row = cur.fetchone()
        while row:
            PROD_ID = row[0]
            ORG_PROD_ID = row[1]
            TITLE_MAIN = row[2]
            SALE_PRICE = row[5]
            LIST_PRICE = row[6]
            CUST_ID = row[8]
            CUID = row[10]
            NICKNAME = row[9]
            SALE_DISC = row[7]
            PROD_RANK = row[3]
            VIDEO_ID = row[12]
            WELFARE_ID = row[14]
            TOTAL_NUM_ROWS = row[15]
            TOTAL_NUM_PAGES = row[17]
            
            user = Object()
            user.PROD_ID = PROD_ID
            user.ORG_PROD_ID = ORG_PROD_ID

            user.SALE_PRICE = SALE_PRICE
            user.LIST_PRICE = LIST_PRICE
            user.TITLE_MAIN = TITLE_MAIN
            user.CUST_ID = CUST_ID
            user.CUID = CUID
            user.NICKNAME = NICKNAME
            user.SALE_DISC = SALE_DISC
            user.PROD_RANK = get_book_rank(PROD_RANK)
            user.VIDEO_URL = get_video_url(VIDEO_ID)
            user.WELFARE_ID = WELFARE_ID
            user.TOTAL_NUM_ROWS = TOTAL_NUM_ROWS
            user.TOTAL_NUM_PAGES = TOTAL_NUM_PAGES
            
            userinfo.append(user)
    
            row = cur.fetchone()
    except cx_Oracle.DatabaseError as e:
        con.rollback()
        error, = e.args
        logging.error(error.code)
        logging.error(error.message)
        logging.error(error.context)
        msg_log = "getwelfarebook FAIL : %s" % (error.message) 
        logging.info(msg_log)
        raise HTTPException(status_code=401, detail="Bad getwelfarebook")
       
    finally:
        cur.close()
        con.close()

    return userinfo

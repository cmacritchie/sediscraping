import time                     # for measuring the duration of the scrape
import requests                 # this is for using the requests
from bs4 import BeautifulSoup   # this is to use beautiful soup web scraping
import MySQLdb                  #
conn = MySQLdb.connect(host = "localhost",
                       user = "Craig",
                     passwd = "swordfish",
                         db = "insider_information")
cursor = conn.cursor()

def scrapeRange(yearFrom, yearTo):
    url = "https://www.sedi.ca/sedi/SVTItdController"

    querystring = {"locale":"en_CA"}

    monthFrom = 0      #January, first month of year
    dayFrom = 1        #First Day of Month
    #yearFrom =1980     #starting date


    monthTo =11        #December, Last Month of year
    dayTo = 31         #Last day of year
    #yearTo = yearFrom  #same year

    for year in range(yearFrom, yearTo + 1):
        payload = "pageName=com%2Fsedi%2Fjsp%2Freports%2FItdReportInput.jsp&" \
                  "locale=en_CA&" \
                  "jspSynchronizerToken=jdaKtyUw_SlNygLfwu8kp701498333527371&" \
                  "PUBLIC_USER=0&" \
                  "FORMAT=HTML&" \
                  "SORTED_BY=0&" \
                  "SELECT_TYPE=0&" \
                  "SELECT_TYPE_VALUE=&" \
                  "SELECT_TYPE_VALUE_SEARCH_TYPE=3&" \
                  "DATE_RANGE_TYPE=0&" \
                  "MONTH_FROM_PUBLIC={}&" \
                  "DAY_FROM_PUBLIC={}&" \
                  "YEAR_FROM_PUBLIC={}&" \
                  "MONTH_TO_PUBLIC={}&" \
                  "DAY_TO_PUBLIC={}&" \
                  "YEAR_TO_PUBLIC={}&" \
                  "INDUSTRY_CLASSIFICATION=-None-&" \
                  "Search=Search".format(monthFrom, dayFrom, year, monthTo, dayTo, year)
        headers = {
            'host': "www.sedi.ca",
            'cache-control': "no-cache",
            'origin': "https://www.sedi.ca",
            'upgrade-insecure-requests': "1",
            'content-type': "application/x-www-form-urlencoded",
            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'referer': "https://www.sedi.ca/sedi/SVTItdController?locale=en_CA",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "en-US,en;q=0.8",
            'postman-token': "7e15db5a-7a8f-b3cb-5539-7bd905217675"
            }

        response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
        soup = BeautifulSoup(response.text, "html.parser")  #html.parser, lxml, lxml.html

        #print(response.text)
        #print(soup.title.string)
        for app in soup.findAll('table', {'bgcolor' : '#CCCCCC'}):   #inserts tag into feature for further processing
            app['value'] = "new"
           # print(app)


        #Scapes web page
        for new in soup.findAll('td', {'width' : '1415'}):   #finds Issuer Name
            issuer = new.text.strip()
            print(issuer)
            insName = new.find_next('td', {'width' : '1451'})#finds Insider Name
            xinsName = insName.text.strip()
            print(xinsName)
            cease = new.find_next('td', {'width' : '482'})   #finds cease to be insider
            xcease = cease.text.strip()
            print(xcease)
            parent = cease.find_parent("table")                      #changes 'cursor' to parent table
            while True:
                tran = parent.find_next('table', {'width': '1680'})  #finds next transaction if table is a transaction
                #TEst
                if tran is None:
                    #print("finished")
                    break
                else:
                    tranId = tran.find('td', {'width': '80'})       #finds transaction id
                    #par = tran.find_parent("table")                 #finds parent table of transaction id
                    if tran.has_attr('value'):                      #if table is a new Issuer, exit
                        break
                    elif tranId is None:                            #if the table does not contain transactions move to next table
                        #print("nothing here")
                        #tt = tran.find_parent('table', {'width': '1680'})
                        parent = tran
                    else:                                           #table contains transaction information, record and move to next
                        #print(tranId.text.strip())
                        #insert additional info
                        dot = tranId.find_next('td', {'width': '104'})  #date of transaction
                        dof = dot.find_next('td', {'width':'104'})      #date of filing
                        ot = dof.find_next('td', {'width' : '114'})     #Ownership Type
                        NoT = ot.find_next('td', {'width' : '154'})
                        cb = NoT.find_next('td', {'width' : '82'})

                        xtranId = tranId.text.strip()
                        xdot = dot.text.strip()
                        xdof = dof.text.strip()
                        xot = ot.text.strip()
                        xNoT  = NoT.text.strip()
                        xcb = cb.text.strip()

                        print("%s, %s, %s, %s, %s, %s" % (xtranId, xdot, xdof, xot, xNoT, xcb))
                        try:
                            cursor.execute("INSERT INTO tran_detail VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s)",
                                           (issuer, xinsName, xcease, xtranId, xdot, xdof, xot, xNoT, xcb))
                            conn.commit()
                        except:
                            conn.rollback()


                        #insert additional info
                        par2 = tranId.find_parent("table")
                        parent = par2


def insiderFind(issuer): #put issuer inside
    #issuer = "Transcontinental Inc."

    url = "https://www.sedi.ca/sedi/SVTSelectSediInsider"

    querystring = {"menukey":"null","locale":"en_CA"}

    payload = "pageName=com%2Fsedi%2Fjsp%2Finsider%2FselectSediInsider.jsp&" \
              "menukey=null&locale=en_CA&" \
              "jspSynchronizerToken=N2NtoCwYi2nwljSh3PFtfrE1498608588598&PUBLIC_SEARCH=setFlag&" \
              "INSIDER_STATE=1&" \
              "INSIDER_KEY=&" \
              "INSIDER_LNAME=&" \
              "INSIDER_FAMILY_NAME_SEARCH_TYPE=2&" \
              "INSIDER_FNAME=&" \
              "INSIDER_GIVEN_NAME_SEARCH_TYPE=2&" \
              "INS_CORP_NAME=&" \
              "INSIDER_COMPANY_NAME_SEARCH_TYPE=2&" \
              "ISSH_NIPDB=&" \
              "issuer_name={}&" \
              "ISSUER_NAME_SEARCH_TYPE=2&" \
              "Search=Search".format(issuer)
    headers = {
        'cache-control': "no-cache",
        'content-type': "application/x-www-form-urlencoded",
        'postman-token': "7e9dfae8-eb7b-b376-7d1e-a3ba9aac2000"
        }

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    soup =  BeautifulSoup(response.text, "html.parser")

    i = 0
    for invs in soup.findAll('input', {'type' : 'RADIO'}):
        insideID =invs['value']
        insider =invs.find_next('font').text.strip()
        i = i +1
        try:
            cursor.execute("INSERT INTO insider VALUES(%s, %s)", (insideID, insider))
            conn.commit()
        except:
            conn.rollback()
        print(insideID)
        print(insider)

    print(i)
    print("done")

def informationScrape(insiderID):

    url = "https://www.sedi.ca/sedi/SVTSelectSediInsider"

    querystring = {"menukey": "null", "locale": "en_CA"}

    payload = "pageName=com%2Fsedi%2Fjsp%2Finsider%2FselectSediInsiderSearchResults.jsp&menukey=null&" \
              "locale=en_CA&" \
              "jspSynchronizerToken=60QoCi6Szj_OsOjsHkqKx3w1498601687541&" \
              "UID_INSPR={}&" \
              "Next=Next".format(insiderID)
    headers = {
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        'postman-token': "6af417cf-4f03-f9ba-e56c-38b4a0e80813"
    }

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    soup = BeautifulSoup(response.text, "html.parser")

    sName = soup.find('td', {'width' : '54%'})
    xsName =sName.text.strip()
    gName = sName.find_next('td', {'width' : '54%'})
    xgName = gName.text.strip()
    Name = xsName + ", " + xgName
    muni = gName.find_next('td', {'width' : '54%'})
    xmuni = muni.text.strip()
    prov = muni.find_next('td', {'width' : '54%'})
    xprov = prov.text.strip()
    country = prov.find_next('td', {'width' : '54%'})
    xcountry = country.text.strip()
    print(Name)
    print(xmuni)
    print(xprov)
    print(xcountry)
    while True:
        issue = country.find_next('b')
        if issue is None:
            print("done")
            break
        else:
            issueNum = issue.find_next('td', {'width' : '54%'})
            xissueNum = issueNum.text.strip()
            issueName = issueNum.find_next('td', {'width' : '54%'})
            xissueName = issueName.text.strip()
            relation = issueName.find_next('td', {'width' : '54%'})
            xrelation = ""
            for rel in relation.find_all('font'):
                xrelation = rel.text.strip() + ', ' + xrelation

            #xrelation = clean.text.strip()
            dateInsideIssuer = relation.find_next('td', {'width':'54%'})
            xdateInIs = dateInsideIssuer.text.strip()
            openBalDate = dateInsideIssuer.find_next('td', {'width':'54%'})
            xopenBalDate = openBalDate.text.strip()
            DateInsideCease = openBalDate.find_next('td', {'width':'54%'})
            xDateInsideCease = DateInsideCease.text.strip()

            print(xissueNum)
            print(xissueName)
            print(xrelation)
            print(xdateInIs)
            print(xopenBalDate)
            print(xDateInsideCease)
            try:
                cursor.execute("INSERT INTO insider_detail VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               (Name, xmuni, xprov, xcountry, xissueNum, xissueName, xrelation, xdateInIs, xopenBalDate, xDateInsideCease))
                conn.commit()
            except:
                conn.rollback()

            country = DateInsideCease


#start program
if __name__ == "__main__":
    start = time.time()
    yearFrom = 1989
    yearTo = 1990
    if yearFrom > yearTo:
        print("please select a date year-To date that is greater than or equal to year-From date")
    else:
        scrapeRange(yearFrom, yearTo)
        issueNameQuery = ("SELECT DISTINCT ISSUE_NAME FROM tran_detail")
        cursor.execute(issueNameQuery)
        for item in list(list(cursor.fetchall())):
            prep1 = str(item)
            prep2 = (prep1.replace("('", "")).replace("',)", "")
            insiderFind(prep2)

        insiderQuery = ("SELECT DISTINCT INSIDER_ID FROM INSIDER")
        cursor.execute(insiderQuery)
        for inside in list(list(cursor.fetchall())):
            prep3 = str(inside)
            prep4 = (prep3.replace("(", "")).replace("L,)", "")
            informationScrape(prep4)

        conn.close()
        cursor.close()
        print("Finished Scrape in %s")
    end = time.time()
    duration = end - start
    print("task finished in {} seconds ".format(duration))





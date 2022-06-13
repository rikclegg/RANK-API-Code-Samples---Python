# rankDemoReport.py

''' 
This sample program demonstrates the use of the RANK API product, along with the 
Reference Data service, to construct a periodic PDF report from a known security
universe.
'''

import sys
import getopt
from typing import overload
import blpapi
import csv
from datetime import date, timedelta, datetime
import os
from fpdf import FPDF
import subprocess

EXCEPTION                       = blpapi.Name("Exception")
REPORT                          = blpapi.Name("Report")

d_host = 'localhost'
d_port = 8194
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def import_securities(sec_file):

    with open(os.path.join(__location__, sec_file)) as csv_file:
        sec_reader = csv.reader(csv_file, delimiter=',')
        sec_list = list(sec_reader)
        sec_list.pop(0) # remove the title
        return sec_list

def import_analyst_mappings(analysts_file):
    
    with open(os.path.join(__location__, analysts_file)) as csv_file:
        af_reader = csv.reader(csv_file, delimiter=',')
        af_list = list(af_reader)
        af_list.pop(0) # remove the title
        return af_list


def get_rank_data(session, securities, iso_date, broker, analyst_mappings):

    print("Retrieving security rankings...")

    rank_data = []

    data_security = ''
    data_rank = 0
    data_volume = 0
    data_sector = ''
    data_analyst = ''
    data_lasttrade = 0.0

    rankapi = session.getService("//blp/rankapi")
    refdataapi = session.getService("//blp/refdata")
    
    i=0
    
    for in_sec in securities:

        i=i+1

        data_security = in_sec[0]

        s = "Retrieving " + str(i) + " of " + str(len(securities)) + " security rankings (" + data_security + ")                                       "
        print(s, end="\r")
        #print(s)

        rank_request = rankapi.createRequest("Query")

        rank_request.set("start", iso_date)
        rank_request.set("end", iso_date)

        rank_request.set("units", "Shares")             
        rank_request.set("source", "Broker Contributed")
        rank_request.set("groupBy", "Broker")

        sec_el = rank_request.getElement("securityCriteria").setChoice("securities")
        sec_el.appendElement().setElement("ticker", data_security)

        bkr_el = rank_request.getElement("brokers").appendElement()
        bkr_el.setElement("acronym", broker)

        #print(rank_request)

        session.sendRequest(rank_request)

        done = False
        while not done:
            event = session.nextEvent(500)

            if event.eventType() == blpapi.Event.RESPONSE:

                for msg in event:   
                    
                    #print(msg)

                    if msg.messageType() == EXCEPTION:
                        print ("Exception occured.")
                        return [], False    
                
                    elif msg.messageType() == REPORT:

                        records = msg.getElement("records")
                        for record in records.values():

                            data_rank = record.getElement("broker").getElement("rank").getValueAsInteger()
                            data_volume = record.getElement("total").getValueAsInteger()

                        done = True

        data_analyst = get_analyst(data_security, analyst_mappings)

        refdata_request = refdataapi.createRequest("ReferenceDataRequest")
        refdata_request.append("securities", data_security)
        refdata_request.append("fields", "DS199")
        refdata_request.append("fields", "PR088")

        #print(refdata_request)

        session.sendRequest(refdata_request)

        done = False
        while not done:
            event = session.nextEvent(500)

            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:   
                    #print(msg)
                    sec_data = msg.getElement("securityData")
                    for sd in sec_data.values():
                        if sd.getElement("fieldData").hasElement("DS199"):
                            data_sector = sd.getElement("fieldData").getElement("DS199").getValueAsString()
                        else:
                            data_sector = "(unknown)"

                        

                        data_lasttrade = sd.getElement("fieldData").getElement("PR088").getValueAsFloat()

                done = True

    
        rank_data.append([data_security, data_rank, data_volume, data_sector, data_analyst, data_lasttrade])

    print("Retrieved " + str(len(securities)) + " security rankings")

    return rank_data, True

def get_analyst(sec, analyst_mappings):

    for l in analyst_mappings:
        if l[0] == sec:
            return l[1]
    
    return ''


def get_position_data(session, securities, iso_date, broker):

    print("Retrieving broker rank data...")

    pos_data=[]
    full_pos_data = []
    overall_total=0

    rankapi = session.getService("//blp/rankapi")

    rank_request = rankapi.createRequest("Query")

    rank_request.set("start", iso_date)
    rank_request.set("end", iso_date)

    rank_request.set("units", "Shares")             
    rank_request.set("source", "Broker Contributed")
    rank_request.set("groupBy", "Broker")

    sec_el = rank_request.getElement("securityCriteria").setChoice("securities")
    for in_sec in securities:
        sec_el.appendElement().setElement("ticker", in_sec[0])

    #print(rank_request)
    
    session.sendRequest(rank_request)

    done = False
    while not done:
        event = session.nextEvent(500)

        if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:

            for msg in event:   
                
                #print(msg)

                if msg.messageType() == EXCEPTION:
                    print ("Exception occured.")
                    return [], False    
            
                elif msg.messageType() == REPORT:

                    records = msg.getElement("records")

                    for record in records.values():
                    
                        #print ("Record: \n", record)
                        broker_acronym = record.getElement("broker").getElement("acronym").getValueAsString()
                        broker_name = record.getElement("broker").getElement("name").getValueAsString()
                        broker_rank = record.getElement("broker").getElement("rank").getValueAsInteger()
                        total_traded = record.getElementAsFloat("traded")

                        full_pos_data.append([broker_acronym, broker_name, broker_rank, total_traded])
                        overall_total+=total_traded

                    if event.eventType() == blpapi.Event.RESPONSE:
                        done = True

        print("Broker ranking retieved...establishing overall ranking")
        
        for i in range(0,(len(full_pos_data)-1)):
            if full_pos_data[i][0] == broker:
                
                if i==0: # broker is ranked #1
                    pos_data.append([])
                else:
                    pos_data.append(full_pos_data[i-1])
                
                pos_data.append(full_pos_data[i])

                if i < (len(full_pos_data)-1):
                    pos_data.append(full_pos_data[i+1])

    print("Overall position established")

    return pos_data, overall_total,True

def generate_output(output_file, position_data, rank_data, overall_total, iso_date):

    print('Generating PDF output file...', end='\r')

    fpdf = FPDF(orientation='P', unit="mm", format='A4')

    fpdf.add_page()

    fpdf.image(r'C:\Users\CleggRichard\Development\RANK API\RANK-API-Code-Samples\Python\yourlogohere.png', 10, 0, 60, 20)

    fpdf.set_fill_color(40, 76, 125)
    fpdf.set_draw_color(40, 76, 125)
    
    fpdf.rect(0,25,210,10, style='FD')

    fpdf.set_text_color(255,255,255)
    fpdf.set_font(family='Arial', style='B', size=12)

    fpdf.set_xy(2,25)
    fpdf.cell(30,5,"Daily Trading Recap:")

    fpdf.set_xy(90,25)
    fpdf.cell(50,5, datetime.strptime(iso_date,"%Y-%m-%d").strftime("%A, %B %#d, %Y"))

    fpdf.set_xy(2,30)
    fpdf.set_font(family='Arial',size=10)
    fpdf.cell(50,5, "Rankings based on trading activity")

    fpdf.set_text_color(0,0,0)
    fpdf.set_font(family='Arial', style='B', size=10)

    fpdf.set_xy(2,40)
    fpdf.cell(65,5, "Total Institutional Trading Volume: ")
    fpdf.set_font(family='Arial', size=10)
    fpdf.cell(30,5, str(int(overall_total)))

    if not position_data[0]==[]:
        fpdf.set_xy(10,50)
        fpdf.set_font(family='Arial',size=10)
        fpdf.cell(30,5, "Ranked")
        fpdf.cell(10,5,str(position_data[0][2]))
        fpdf.cell(35,5,str(int(position_data[0][3])))
        fpdf.cell(10,5,'('+str(position_data[0][1])+')')
    
    fpdf.set_xy(10,55)
    fpdf.set_font(family='Arial',style='B',size=12)
    fpdf.cell(30,5, "Ranked")
    fpdf.cell(10,5,str(position_data[1][2]))
    fpdf.cell(35,5,str(int(position_data[1][3])))
    fpdf.cell(10,5,'('+str(position_data[1][1])+')')

    if not position_data[2]==[]:
        fpdf.set_xy(10,60)
        fpdf.set_font(family='Arial',size=10)
        fpdf.cell(30,5, "Ranked")
        fpdf.cell(10,5,str(position_data[2][2]))
        fpdf.cell(35,5,str(int(position_data[2][3])))
        fpdf.cell(10,5,'('+str(position_data[2][1])+')')


    fpdf.set_xy(2,70)
    fpdf.set_font(family='Arial',style='B',size=12)
    fpdf.cell(65,5, "Top 30 ADV (with Rank and Industry Group)")

    fpdf.rect(0,70,210,10, style='FD')

    fpdf.set_text_color(255,255,255)
    fpdf.set_font(family='Arial',size=10)

    fpdf.set_xy(2,75)
    fpdf.cell(40,5,"Security")
    fpdf.cell(30,5,"Rank")
    fpdf.cell(30,5,"Broker Volume")
    fpdf.cell(50,5,"Industry Sector")
    fpdf.cell(30,5,"Analyst")
    fpdf.cell(30,5,"Last Trade")

    fpdf.set_text_color(0,0,0)
    fpdf.set_xy(2,80)
    fpdf.set_left_margin(2)

    for rnk in rank_data:
        fpdf.ln(5)
        fpdf.cell(40,5,rnk[0])
        fpdf.cell(30,5,str(rnk[1]))
        fpdf.cell(30,5,"{:,}".format(rnk[2]))
        fpdf.cell(50,5,rnk[3])
        fpdf.cell(30,5,rnk[4])
        fpdf.cell(30,5,'$'+str(rnk[5]))

    
    of = os.path.join(__location__, output_file)
    fpdf.output(of,'F')

    print('PDF output file created: ' , of)


def main(argv):
    
    sec_uni = ''
    iso_date = ''
    output_file = ''
    analysts_file = ''
    broker = ''

    usage = 'rankDemoReport.py -s <securities file> -d <ISO date> -a <analyst mapping file> -o <output file>'
    
    try:
        opts, args = getopt.getopt(argv,"hs:d:a:o:b:",["help","securities=","date=","analysts=","output=","broker="])
    except getopt.GetoptError:
        print (usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print (usage)
            sys.exit()
        elif opt in ("-s", "--securities"):
            sec_uni = arg
        elif opt in ("-b", "--broker"):
            broker = arg
        elif opt in ("-d", "--date"):
            iso_date = arg
        elif opt in ("-a", "--analysts"):
            analysts_file = arg
        elif opt in ("-o", "--output"):
            output_file = arg
   
    if sec_uni == '':
        print("Error: missing security source")
        sys.exit(2)

    if broker == '':
        print("Error: missing broker code")
        sys.exit(2)

    if iso_date == '':
        print("Assuming T-1")

        today = date.today()
        yesterday = today - timedelta(days = 1)
        iso_date = yesterday.isoformat()

    if analysts_file == '':
        print("No analyst mappings provided")

    if output_file =='':
        print('Defaulting output file')
        output_file = "report_" + iso_date + ".pdf"


    print ('\nSecurities file: ', sec_uni)
    print ("Broker: ", broker)
    print ('ISO Date: ', iso_date)
    print ('Analysts: ', "None" if analysts_file=='' else analysts_file)
    print ('Output file:', output_file)

    # Connect to Bloomberg services
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host) # This represents the chose connectivity method. In this case, we are using Desktop API, so the host is 'localhost'
    sessionOptions.setServerPort(d_port) # The default port is 8194.
    
    print ("Connecting to %s:%d" % (d_host, d_port))

    session = blpapi.Session(sessionOptions)

    if not session.start():
        print ("Error: Failed to start session.")
        sys.exit(2)

    if not session.openService("//blp/rankapi"):
        print("Failed to open RANK API service")
        sys.exit(2)

    print("RANK API service opened.")

    if not session.openService("//blp/refdata"):
        print("Failed to open Reference Data API service")
        sys.exit(2)

    print("Reference Data service opened.")

    securities = import_securities(sec_uni)

    analyst_mappings = import_analyst_mappings(analysts_file)

    rank_data, success = get_rank_data(session, securities, iso_date, broker, analyst_mappings)

    if success==False:
        sys.exit(2)

    print ("Full Rank data:")
    for r in rank_data:
        print(r)

    position_data, overall_total, success = get_position_data(session, securities, iso_date, broker)

    if success==False:
        sys.exit(2)

    print (position_data)
    print ("Overall total: ", overall_total)

    generate_output(output_file, position_data, rank_data, overall_total, iso_date)

    print("Finshed.")

    of = os.path.join(__location__, output_file)

    print("Opening PDF: ", of)
    
    #os.startfile(os.path.join(__location__, output_file))
    #subprocess.Popen(['start', os.path.join(__location__, output_file)], shell=True)
    os.system('"' + of + '"')

    print("Terminating.")
    

if __name__ == "__main__":
    print ("Bloomberg - RANK API Demo Report - rankDemoReport")
    main(sys.argv[1:])


__copyright__ = """
Copyright 2022. Bloomberg Finance L.P.
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:  The above
copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
"""





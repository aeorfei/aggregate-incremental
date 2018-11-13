def ConvertBaselineJson(siteUuidList):
    userUuidCards = pd.DataFrame()
    for siteUuid in tqdm(siteUuidList):
        SiteuserUuidCards = GetBaselineJson([siteUuid])
        if len(SiteuserUuidCards) == 0:
            continue
        columns = [col for col in SiteuserUuidCards if col.startswith('cardIds')]
        melted = SiteuserUuidCards[columns + ['userUuid','enrollDate']].melt(id_vars=['userUuid','enrollDate'])
        melted = melted.drop(columns=['variable'])
        melted = melted.dropna(subset=['value'])
        melted['cardType'] = melted.value.apply(lambda x:x.split('-')[0])
        melted['cardFirstSix'] = melted.value.apply(lambda x:x.split('-')[1])
        melted['cardLastFour'] = melted.value.apply(lambda x:x.split('-')[2])
        melted['siteUuid'] = siteUuid
        melted['merchantUuid'] = GetSiteInfo(siteUuid)['merchantUuid']
        melted = melted.rename(columns = {'value':'cardId'})
        userUuidCards = userUuidCards.append(melted,sort=False)
    return userUuidCards

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def GetAllVisibleSites():
    """
    Return a list of sites
    """
    import pandas as pd
    SiteInfo = pd.read_csv('/Users/alessandroorfei/PycharmProjects/aggregate-incremental/resources/gas_merchant_service.csv')
    SiteInfo = SiteInfo[(SiteInfo.visibility == "DEFAULT")].copy()
    return list(SiteInfo['siteUuid'])


def GetBaselineJson(siteUuidList):
    s3 = boto3.resource('s3')

    if type(siteUuidList) != list:
        siteUuidList = [siteUuidList]

    AllSites = pd.DataFrame()

    for siteUuid in tqdm(siteUuidList):
        merchantUuid = GetSiteInfo(siteUuid)['merchantUuid']
        content_object = s3.Object('data.upside-services.com',
                                   'service-station/' + merchantUuid + '/' + siteUuid + '/data/analysis/baseline.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        d = json.loads(file_content)
        SiteuserUuidCards = pd.DataFrame()
        for user in range(0, len(d['userBaselines'])):
            d_flat = flatten_json(d['userBaselines'][user])
            dat = json_normalize(d_flat)
            SiteuserUuidCards['siteUuid'] = siteUuid
            SiteuserUuidCards = SiteuserUuidCards.append(dat, ignore_index=True, sort=False)
        AllSites = AllSites.append(SiteuserUuidCards, sort=False)
    return AllSites


def GetIncremental(siteUuidList, StartDate, EndDate, userUuidList=[]):
    """
      This function returns a dataframe of Incremental data for a siteUuid
      parameters:
          siteUuidList:  site identifiers. e.g. ['e30a6caa-efdd-4d5d-92ad-010d1d158a35']
          StartDate: string date, e.g. "2018-04-01"
          EndDate: string date, e.g. "2018-10-31"
      returns:
          DataFrame with Incremental date converted to datetime
      """
    import os
    import pandas as pd
    os.system('pip2 install --upgrade runbookcli')
    os.chdir('/Users/alessandroorfei/Desktop/')

    if type(siteUuidList) != list:
        siteUuidList = [siteUuidList]

    Incremental = pd.DataFrame()
    for siteUuid in tqdm(siteUuidList):
        incremental_downloader = 'runbook get_incremental prod ' + 'incremental_' + siteUuid + '.csv --sites ' + siteUuid
        print(incremental_downloader)
        os.system(incremental_downloader)
        SiteIncremental = pd.read_csv('/Users/alessandroorfei/Desktop/' + 'incremental_' + siteUuid + '.csv')
        SiteIncremental['date'] = pd.to_datetime(SiteIncremental.date)
        SiteIncremental = SiteIncremental[(SiteIncremental.date >= pd.to_datetime(StartDate))
                                          & (SiteIncremental.date <= pd.to_datetime(EndDate))].copy()

        # Filter to permitted users
        if userUuidList != []:
            SiteIncremental = SiteIncremental[SiteIncremental.userUuid.isin(userUuidList)].copy()

        os.system("rm /Users/alessandroorfei/Desktop/incremental_" + siteUuid + '.csv')
        Incremental = Incremental.append(SiteIncremental, sort=False)
    return Incremental


def GetMerchantSites(merchantUuidList, visibility="DEFAULT", processorType=[]):
    """
    Return a list of sites
    """
    import pandas as pd
    SiteInfo = pd.read_csv('/Users/alessandroorfei/PycharmProjects/aggregate-incremental/resources/gas_merchant_service.csv')
    SiteInfo = SiteInfo[(SiteInfo.merchantUuid.isin(merchantUuidList)) &
                        (SiteInfo.visibility == visibility)].copy()

    if processorType != []:
        SiteInfo = SiteInfo[SiteInfo.processorType.isin(processorType)].copy()

    return list(SiteInfo['siteUuid'])

def GetpersonID(df, PersonIdentifiers):
    df['personID'] = ""
    for var in PersonIdentifiers:
        df['personID'] = df['personID'] + df[var].astype('str')
    return df['personID']


def GetSiteInfo(siteUuid, visibility = "DEFAULT"):
    """
    Returns a dict object with info on the site
    """
    import pandas as pd
    SiteInfo = pd.read_csv('/Users/alessandroorfei/PycharmProjects/aggregate-incremental/resources/gas_merchant_service.csv')
    SiteInfo = SiteInfo[(SiteInfo.siteUuid == siteUuid) &
                        (SiteInfo.visibility == visibility)].copy()
    assert len(SiteInfo) == 1
    SiteInfo = SiteInfo.to_dict(orient='records')[0]
    return SiteInfo


def GetTransactions(siteUuidList, StartDate, EndDate, sourceTerminal=["All"]):
    """
    This function returns a dataframe of Transaction data for siteUuid
    parameters:
        siteUuidList:  list of site identifiers. e.g. ['e30a6caa-efdd-4d5d-92ad-010d1d158a35']
        StartDate: string date, e.g. "2018-04-01"
        EndDate: string date, e.g. "2018-10-31"
    returns:
        DataFrame with Transaction data plus TranTime and TranDate
    """
    import boto3
    import pandas as pd
    from upside_core.transaction.datalake_dao import TransactionDataLakeDAO
    from pandas.io.json import json_normalize  # package for flattening json in pandas df

    StartDate = pd.to_datetime(StartDate)
    EndDate = pd.to_datetime(EndDate)

    Transactions = pd.DataFrame()
    for siteUuid in tqdm(siteUuidList):
        transaction_dao = TransactionDataLakeDAO(tier='prod', s3_client=boto3.client('s3'))
        SiteTransactions = transaction_dao.get(siteUuid, 'default', StartDate, EndDate)
        SiteTransactions = json_normalize(SiteTransactions)
        SiteTransactions['TranTime'] = pd.to_datetime(SiteTransactions['transactionTimestamp'])
        SiteTransactions['TranDate'] = pd.to_datetime(SiteTransactions.TranTime.dt.date)

        if sourceTerminal != ["All"]:
            SiteTransactions = SiteTransactions[SiteTransactions.sourceTerminal.isin(sourceTerminal)].copy()
        Transactions = Transactions.append(SiteTransactions, sort=False)

    return Transactions


def GraphTranCounts(Transactions, StartDate, EndDate):
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    counts = pd.DataFrame(Transactions.TranDate.value_counts().sort_index())
    counts = counts.reset_index()
    counts = counts.rename(columns={'index': 'TranDate',
                                    'TranDate': 'counts'})

    spine = pd.DataFrame(pd.date_range(start=StartDate, end=EndDate), columns={'TranDate'})
    fullcount = pd.merge(spine, counts, how='left', on='TranDate')
    fullcount['counts'] = np.where(fullcount.counts.isnull(), 0, fullcount.counts)
    fullcount = fullcount.set_index('TranDate')
    ax = fullcount.plot()
    ax.set_ylim(ymin=0)
    return

def ImputeInsideTransactions(Transactions, processor, SalesTax):
    """
    Returns a Series that is the inferred inside purchase amount
    based on observing the tax and knowing the sales tax
    """
    import numpy as np
    if processor == "motiva":
        Transactions['InsideAmount'] = np.where(Transactions.sourceTerminal == "INSIDE",
                                                Transactions['total.amount'],
                                                np.nan)
    else:
        Transactions['InsideAmount'] = np.where(Transactions.sourceTerminal == "INSIDE",
                                                 Transactions['tax.amount']/SalesTax,
                                                 np.nan)
    return Transactions['InsideAmount']


def MatchOfferAndTx(Incremental, userUuidCards, Transactions):
    import pandas as pd

    Incremental = pd.merge(Incremental,
                           userUuidCards,
                           how='left',
                           on=['siteUuid', 'userUuid'])

    # ## First Pass

    Incremental = Incremental.drop(columns=['merchantUuid'])

    # Pass Number 1 - Matchiing on: ['TranDate','siteUuid','cardId','total.amount','sourceTerminal'])
    TotalMatches = pd.DataFrame()
    MatchingIncPass1 = pd.DataFrame()
    for i in [col for col in Incremental if col.startswith('cardIds_')]:
        Matched = pd.merge(Incremental,
                           Transactions,
                           how='inner',
                           left_on=['date', 'siteUuid', i, 'totalRevenue', 'sourceTerminal'],
                           right_on=['TranDate', 'siteUuid', 'cardId', 'total.amount', 'sourceTerminal'])

        MatchingIncPass1 = MatchingIncPass1.append(Matched, sort=False)
    MatchingIncPass1 = MatchingIncPass1.drop_duplicates(subset=['transactionUuid'])
    MatchingIncPass1 = MatchingIncPass1.drop_duplicates(subset=['offerUuid'])

    TotalMatches = TotalMatches.append(MatchingIncPass1, sort=False)
    TotalMatches = TotalMatches.drop_duplicates(subset=['transactionUuid'])
    TotalMatches = TotalMatches.drop_duplicates(subset=['offerUuid'])

    print(len(TotalMatches))
    print(len(Incremental))
    print("Failed to match on Pass 1 ", 1 - len(TotalMatches) / len(Incremental))

    IncNotMatched_Pass1 = Incremental[~Incremental.offerUuid.isin(TotalMatches.offerUuid)].copy()
    TransNotMatched_Pass1 = Transactions[~Transactions.transactionUuid.isin(TotalMatches.transactionUuid)].copy()

    # ## Second Pass

    # Pass2 - Matching on: ['TranDate','siteUuid','cardId','sourceTerminal']
    MatchingIncPass2 = pd.DataFrame()

    for i in [col for col in IncNotMatched_Pass1 if col.startswith('cardIds_')]:
        Matched = pd.merge(IncNotMatched_Pass1,
                           TransNotMatched_Pass1,
                           how='inner',
                           left_on=['date', 'siteUuid', i, 'sourceTerminal'],
                           right_on=['TranDate', 'siteUuid', 'cardId', 'sourceTerminal'])

        MatchingIncPass2 = MatchingIncPass2.append(Matched, sort=False)
    MatchingIncPass2 = MatchingIncPass2.drop_duplicates(subset=['transactionUuid'])
    MatchingIncPass2 = MatchingIncPass2.drop_duplicates(subset=['offerUuid'])

    TotalMatches = TotalMatches.append(MatchingIncPass2, sort=False)
    TotalMatches = TotalMatches.drop_duplicates(subset=['transactionUuid'])
    TotalMatches = TotalMatches.drop_duplicates(subset=['offerUuid'])

    print(len(TotalMatches))
    print(len(Incremental))
    print("Failed to match on Pass 2 ", 1 - len(TotalMatches) / len(Incremental))

    IncNotMatched_Pass2 = Incremental[~Incremental.offerUuid.isin(TotalMatches.offerUuid)].copy()
    TransNotMatched_Pass2 = Transactions[~Transactions.transactionUuid.isin(TotalMatches.transactionUuid)].copy()

    # ## Third Pass

    # Pass3 - exact match on date, siteUuid, cardId, sale amount

    MatchingIncPass3 = pd.DataFrame()

    IncNotMatched_Pass2 = IncNotMatched_Pass2.drop(columns=['sourceTerminal'])

    for i in [col for col in IncNotMatched_Pass2 if col.startswith('cardIds_')]:
        Matched = pd.merge(IncNotMatched_Pass2,
                           TransNotMatched_Pass2,
                           how='inner',
                           left_on=['date', 'siteUuid', i, 'totalRevenue'],
                           right_on=['TranDate', 'siteUuid', 'cardId', 'total.amount'])

        MatchingIncPass3 = MatchingIncPass3.append(Matched, sort=False)
    MatchingIncPass3 = MatchingIncPass3.drop_duplicates(subset=['transactionUuid'])
    MatchingIncPass3 = MatchingIncPass3.drop_duplicates(subset=['offerUuid'])

    TotalMatches = TotalMatches.append(MatchingIncPass3, sort=False)
    TotalMatches = TotalMatches.drop_duplicates(subset=['transactionUuid'])
    TotalMatches = TotalMatches.drop_duplicates(subset=['offerUuid'])

    print(len(TotalMatches))
    print(len(Incremental))
    print("Failed to match on Pass 3 ", 1 - len(TotalMatches) / len(Incremental))

    IncNotMatched_Pass3 = Incremental[~Incremental.offerUuid.isin(TotalMatches.offerUuid)].copy()
    TransNotMatched_Pass3 = Transactions[~Transactions.transactionUuid.isin(TotalMatches.transactionUuid)].copy()

    # ## Fourth Pass

    # Pass4 - exact match on date, siteUuid, cardId, sale amount

    MatchingIncPass4 = pd.DataFrame()

    for i in [col for col in IncNotMatched_Pass3 if col.startswith('cardIds_')]:

        Test = IncNotMatched_Pass3[i].str.split(pat="-", expand=True, n=2)

        Test = Test.rename(columns={0: 'cardType_json',
                                    1: 'cardFirstSix',
                                    2: 'cardLastFour'})

        Test = pd.concat([IncNotMatched_Pass3, Test], sort=False, axis=1)

        if len(Test[Test[i].notnull()]) == 0:
            continue

        Matched = pd.merge(Test,
                           TransNotMatched_Pass3,
                           how='inner',
                           left_on=['date', 'siteUuid', 'cardFirstSix', 'cardLastFour', 'totalRevenue',
                                    'sourceTerminal'],
                           right_on=['TranDate', 'siteUuid', 'cardFirstSix', 'cardLastFour', 'total.amount',
                                     'sourceTerminal'])

        MatchingIncPass4 = MatchingIncPass4.append(Matched, sort=False)
    MatchingIncPass4 = MatchingIncPass4.drop_duplicates(subset=['transactionUuid'])
    MatchingIncPass4 = MatchingIncPass4.drop_duplicates(subset=['offerUuid'])

    TotalMatches = TotalMatches.append(MatchingIncPass4, sort=False)
    TotalMatches = TotalMatches.drop_duplicates(subset=['transactionUuid'])
    TotalMatches = TotalMatches.drop_duplicates(subset=['offerUuid'])

    print(len(TotalMatches))
    print(len(Incremental))
    print("Failed to match on Pass 4 ", 1 - len(TotalMatches) / len(Incremental))

    IncNotMatched_Pass4 = Incremental[~Incremental.offerUuid.isin(TotalMatches.offerUuid)].copy()
    TransNotMatched_Pass4 = Transactions[~Transactions.transactionUuid.isin(TotalMatches.transactionUuid)].copy()

    TotalMatches = TotalMatches[['siteUuid', 'userUuid', 'offerUuid', 'TranTime', 'transactionUuid']].copy()
    return TotalMatches


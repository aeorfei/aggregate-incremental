def OutputInsideViewThrough(siteUuid,
                            InsideViewThrough,
                            userUuidCards,
                            OutsideTransactions,
                            InsideTransactions,
                            OutsideIncremental,
                            TimeWindow,
                            Outpath,
                            MeasurementUuid):
    import pandas as pd

    userUuidCards = userUuidCards.add_prefix('BaselineJson.')

    InsideViewThrough = pd.merge(InsideViewThrough,
                                 userUuidCards,
                                 how='left',
                                 left_on=['siteUuid','userUuid'],
                                 right_on=['BaselineJson.siteUuid','BaselineJson.userUuid'])

    OutsideTransactions = OutsideTransactions.add_prefix('Outside.')
    InsideTransactions = InsideTransactions.add_prefix('Inside.')

    InsideViewThrough = pd.merge(InsideViewThrough,
                                  OutsideTransactions,
                                  how='left',
                                  left_on=['siteUuid','OffertransactionUuid'],
                                  right_on=['Outside.siteUuid','Outside.transactionUuid'])

    InsideViewThrough = pd.merge(InsideViewThrough,
                                  InsideTransactions,
                                  how='left',
                                  left_on=['siteUuid','transactionUuid'],
                                  right_on=['Inside.siteUuid','Inside.transactionUuid'])

    OutsideIncremental = OutsideIncremental.add_prefix('OutsideInc.')
    
    InsideViewThrough = pd.merge(InsideViewThrough,
                              OutsideIncremental,
                              how='left',
                              left_on=['siteUuid','offerUuid'],
                              right_on=['OutsideInc.siteUuid','OutsideInc.offerUuid'])

    InsideViewThrough['TimeWindow'] = TimeWindow
    path = Outpath  + GetSiteInfo(siteUuid)['merchantUuid'] + '/' + siteUuid + '/'
    
    try:
      os.makedirs(path)
    except:
      print("Directory exists")
    InsideViewThrough.to_csv(path + 'InsideViewThrough_' + str(MeasurementUuid) + '.csv', index=False)
   
    return InsideViewThrough

def PairGUOutsideTxWithInsideTx(OutsideMatchedOffers,
                               userUuidCards,
                               InsideTransactions,
                               TimeWindow):
    import pandas as pd
    """
    MatchedOffers:  Incremental files with TranTime attached
    
    """
    
    OutsideMatchedOffers = pd.merge(OutsideMatchedOffers,
                                  userUuidCards,
                                  how ='left',
                                  on = ['siteUuid','userUuid'])
    
    OutsideMatchedOffers = OutsideMatchedOffers.rename(columns = {'transactionUuid' : 'OffertransactionUuid',
                                                                  'TranTime' : 'OfferTranTime'})
    
    OutsideMatchedOffers.sort_values('OfferTranTime', inplace=True)
    InsideTransactions.sort_values('TranTime', inplace=True)

    InsideViewThrough = pd.DataFrame()
    for i in [col for col in OutsideMatchedOffers if col.startswith('cardIds_')]:
        Hits = pd.merge_asof(OutsideMatchedOffers,
                                InsideTransactions,  
                          left_on = 'OfferTranTime',
                          right_on = 'TranTime',
                          left_by = ['siteUuid',i],
                          right_by = ['siteUuid','cardId'],
                          tolerance = pd.Timedelta(TimeWindow),
                          direction = 'nearest')
        Hits = Hits[Hits.InsideAmount.notnull()].copy()
        InsideViewThrough = InsideViewThrough.append(Hits, sort=False)
        
    InsideViewThrough = InsideViewThrough.drop_duplicates(subset='transactionUuid').copy()
    InsideViewThrough = InsideViewThrough.drop_duplicates(subset='offerUuid').copy()
    InsideViewThrough = InsideViewThrough[InsideViewThrough.InsideAmount > 0].copy()
    
    return InsideViewThrough[['siteUuid','userUuid',
                              'offerUuid','OffertransactionUuid','OfferTranTime',
                              'transactionUuid','TranTime']]

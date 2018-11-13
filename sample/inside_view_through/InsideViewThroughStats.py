def InsideViewThroughStats(siteUuid,
                       OutsideIncremental,
                       MatchedOffers,
                       InsideViewThrough,
                       Transactions,
                       TimeWindow,
                       StartDate,
                       EndDate,
                       SalesTax,
                       Outpath,
                       MeasurementUuid,
                       InsideMargin=0.3):
    from datetime import datetime
    
    Results = json.loads("{}")
    Results['merchantUuid'] = GetSiteInfo(siteUuid)['merchantUuid']
    Results['merchant'] = GetSiteInfo(siteUuid)['merchant']
    Results['siteUuid'] = siteUuid
    Results['processorType'] = GetSiteInfo(siteUuid)['processorType']
    Results['TimeWindow'] = TimeWindow
    Results['StartDate'] = str(StartDate)
    Results['EndDate'] = str(EndDate)
    Results['addedAtTimestamp'] = str(datetime.now())
    Results['MatchedOutsideOffers'] = MatchedOffers.offerUuid.nunique()
    Results['IncrementalOutsideOffers'] = OutsideIncremental.offerUuid.nunique()
    Results['MatchRate'] = Results['MatchedOutsideOffers']/Results['IncrementalOutsideOffers']
    Results['IncOutsideTx'] = OutsideIncremental[OutsideIncremental.totalIncrementalProfit>0]['offerUuid'].nunique()
    Results['IncOutsideProfit'] = OutsideIncremental[OutsideIncremental.totalIncrementalProfit > 0]['totalIncrementalProfit'].sum()
    Results['IncInsideTx'] = InsideViewThrough[InsideViewThrough['OutsideInc.totalIncrementalProfit'] > 0]['Inside.transactionUuid'].nunique()
    Results['IncInsideProfit'] = InsideMargin*InsideViewThrough[InsideViewThrough['OutsideInc.totalIncrementalProfit'] > 0]['Inside.InsideAmount'].sum()
    Results['BasketSizeTimeWindow'] = InsideViewThrough[InsideViewThrough['OutsideInc.totalIncrementalProfit'] > 0]['Inside.InsideAmount'].mean()
    Results['PercentInsideTxTimeWindow'] = Results['IncInsideTx']/Results['IncOutsideTx']
    Results['PercentInsideProfitTimeWindow'] = Results['IncInsideProfit']/Results['IncOutsideProfit']
    Results['AssumedSalesTax'] = SalesTax
    
    if Outpath != "":
      path = Outpath  + GetSiteInfo(siteUuid)['merchantUuid'] + '/' + siteUuid + '/'
      try:
        os.makedirs(path)
      except:
        print("Directory exists")
        
      with open(path + 'Results_' + str(MeasurementUuid) +'.json', 'w') as outfile:
        json.dump(Results, outfile)
    
    return Results

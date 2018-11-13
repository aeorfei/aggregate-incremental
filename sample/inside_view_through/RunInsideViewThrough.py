def RunInsideViewThrough(siteUuid,
                         StartDate,
                         EndDate,
                         TimeWindow,
                         SalesTax,
                         Outpath,
                         AllowedprocessorType = ['motiva']):
    MeasurementUuid = siteUuid
  
    assert GetSiteInfo(siteUuid)['processorType'] in AllowedprocessorType
  
    #Get Data
    Transactions = GetTransactions([siteUuid], StartDate, EndDate)
    OutsideTransactions = Transactions[Transactions.sourceTerminal == "OUTSIDE"].copy()
    InsideTransactions = Transactions[Transactions.sourceTerminal == "INSIDE"].copy()
    
    assert len(InsideTransactions) > 0
    
    userUuidCards =  GetBaselineJson([siteUuid])
    Incremental = GetIncremental(siteUuid, StartDate, EndDate)
    OutsideIncremental = Incremental[Incremental.sourceTerminal == "OUTSIDE"].copy()
    
    # Match offers to transactions
    MatchedOffers = MatchOfferAndTx(OutsideIncremental,
                                userUuidCards,
                                Transactions)
    
    # Infer inside value
    InsideTransactions['InsideAmount'] = ImputeInsideTransactions(InsideTransactions,
                                                         GetSiteInfo(siteUuid)['processorType'], 
                                                         SalesTax)
    
    # Pair Inside and Outside transactions
    InsideViewThrough = PairGUOutsideTxWithInsideTx(MatchedOffers,
                                               userUuidCards,
                                               InsideTransactions,
                                               TimeWindow)
    
    # Create a large table with all the information
    InsideViewThrough = OutputInsideViewThrough(siteUuid,
                                            InsideViewThrough,
                                            userUuidCards,
                                            OutsideTransactions,
                                            InsideTransactions,
                                            OutsideIncremental,
                                            TimeWindow,
                                            Outpath,
                                            MeasurementUuid)
    
    # Summarize the results
    Result = InsideViewThroughStats(siteUuid,
                       OutsideIncremental,
                       MatchedOffers,
                       InsideViewThrough,
                       Transactions,
                       TimeWindow,
                       StartDate,
                       EndDate,
                       SalesTax,
                       Outpath,
                       MeasurementUuid)
    
    return Result

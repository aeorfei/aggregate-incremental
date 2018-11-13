def RunInsideViewThroughMerchant(merchantUuid,
                                 StartDate,
                                 EndDate,
                                 TimeWindow,
                                 SalesTax,
                                 Outpath,
                                 AllowedprocessorType = ['motiva']):

          siteUuidList = GetMerchantSites([merchantUuid])
          Results = pd.DataFrame()
          for siteUuid in tqdm(siteUuidList):
                    try:
                              siteUuidResult = RunInsideViewThrough(siteUuid,
                                                       StartDate,
                                                       EndDate,
                                                       TimeWindow,
                                                       SalesTax,
                                                       Outpath,
                                                       AllowedprocessorType)
                              siteUuidResult = json_normalize(siteUuidResult)
                              Results = Results.append(siteUuidResult, sort=False)
                    except:
                              continue
          return Results
                       


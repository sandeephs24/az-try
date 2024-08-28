CREATE view [dbo].[vwCrossCurrency] as

              SELECT DateQuoted,
                     BaseCurrency, 
                     Currency, 
                     BuyRate = CASE WHEN (BaseCurrency = Currency) THEN 1
                                         WHEN (BaseCurrency = 'GBP' and Currency = 'USD') THEN cast(Bid2 / Ask1 as decimal(25, 4))
                                         WHEN (BaseCurrency = 'EUR' and Currency = 'USD') THEN cast(Bid2 / Ask1 as decimal(25, 6))
                                         WHEN (BaseCurrency = 'EUR') THEN cast(Bid2 / Ask1 as decimal(25, 6))
                                  ELSE cast(Bid2 / Mid1 as decimal(25,4))
                           END,
                     SellRate = CASE WHEN (BaseCurrency = Currency) THEN 1
                                         WHEN (BaseCurrency = 'GBP' and Currency = 'USD') THEN cast(Ask2 / Bid1  as decimal(25, 4))
                                         WHEN (BaseCurrency = 'EUR' and Currency = 'USD') THEN cast(Ask2 / Bid1 as decimal(25, 6))
                                         WHEN (BaseCurrency = 'EUR') THEN cast(Ask2 / Mid1 as decimal(25, 6))
                                  ELSE cast(Ask2 / Mid1 as decimal(25,4))
                           END,
                     MeanRate = CASE WHEN (BaseCurrency = Currency) THEN 1
                                         WHEN (BaseCurrency = 'EUR' and eur.Swift_code is not null) THEN cast(eur.mean_rate as decimal(25, 6))
                                         WHEN (BaseCurrency = 'EUR') THEN cast(Mid2 / Mid1 as decimal(25, 6))
                                         ELSE cast(Mid2 / Mid1 as decimal(25, 4))
                                  END,
                     working_Day as QuoteDay
              from vwExchangeRates_DSM_TMD 
              





CREATE view [dbo].[vwExchangeRates_DSM_TMD] as	-- READ AS THE RESPONSE FROM THE ZEMA API

       select distinct convert(datetime, convert(date, d1.collection_date)) as DateQuoted, d1.Currency as Basecurrency, d2.currency, 
                     convert(decimal(25,6), d1.ask_rate) as ask1, convert(decimal(25,6), d1.bid_rate) as bid1, convert(decimal(25,6), d1.mid_rate) as mid1, d1.working_day, d1.transfer_time,
                     convert(decimal(25,6), d2.ask_rate) as ask2, convert(decimal(25,6), d2.bid_rate) as bid2, convert(decimal(25,6), d2.mid_rate) as mid2
       from   tmd_Data d1, --ZEMA API d1
              tmd_Data d2  --ZEMA API d1
       WHERE  d1.base_currency = d2.base_currency

       
	   
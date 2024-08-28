USE [shell-01-eun-sqdb-koudqbyefeuwrybuauhd]
GO

/****** Object:  StoredProcedure [dbo].[usp_rpt_prod_score_ewindow_solo]    Script Date: 7/14/2021 11:22:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





create  or alter procedure [dbo].[usp_rpt_prod_score_ewindow_solo] ( @table_name varchar(50) ) as
    
	
	declare @lvd_max_date_hist          datetime
	declare @lvd_max_date               datetime
	declare @lvd_max_date_for_source    datetime


	-- =============================================
-- Author:						Vijaya Bhattaru

-- Description:					
-- input:						1. network_pre_stg_platts_na_data_plattsnadatasheet 2. rpt_ewindow_data_hist
-- output tables:				1. rpt_ewindow_data 2. rpt_product_scorecard_eWindow_solo_export 3. product_scorecard_intermediate_ewindow_hist (appends data)
-- parameters:						 @table_name
-- Used in Front end reports: 
-- When should it run           The procedure has to run once after network file plattsnadata gets loaded.
-- usage:                       exec usp_rpt_prod_score_ewindow_solo @table_name = 'rpt_ewindow_data'
-- =============================================

  IF @table_name LIKE 'rpt_ewindow_data%'

  --- output rpt_ewindow_data and rpt_ewindow_data_hist
  -- step1  1. Get the max time from history table. 2. Append data from rpt_ewindow_data to rpt_ewindow_data_hist where the "keep" rule is applicable.
  -- step2  truncate and reload rpt_ewindow_data  from network_pre_stg_platts_na_data_plattsnadatasheet


   BEGIN 

											   -- step 1. 
													 set @lvd_max_date_hist = ( select max(cast( timestampdate as datetime ) ) from product_scorecard_intermediate_ewindow_hist )

											--  

											   truncate table rpt_ewindow_data

												 --- select count(1) from rpt_ewindow_data  -- 20,123 -- mrunali: 20,123
												  -- select count(1) from network_pre_stg_platts_na_data_plattsnadatasheet  -- 20,387
												 --  SELECT COUNT(1) FROM rpt_product_scorecard_eWindow_solo_export  -- 377317 --377,316
												 --- select count(1) from product_scorecard_intermediate_ewindow_hist  -- before today's data merge: 357,194 ; 369,587--  after today's merge: Mrunali:  377,316

												 -- SELECT * FROM rpt_product_scorecard_eWindow_solo_export WHERE ORDERID = 110580122
												 -- SELECT TIMESTAMPDATE, TIMESTAMP FROM network_pre_stg_platts_na_data_plattsnadatasheet WHERE ORDERID = 110580122  -- 2/16/2021 18:51

												 -- SELECT TIMESTAMPDATE, TIMESTAMP FROM network_pre_stg_platts_na_data_plattsnadatasheet WHERE ORDERID = 110580122 

												 -- SELECT * FROM product_scorecard_intermediate_ewindow_hist

												 -- select count(1) from rpt_ewindow_data

												 -- select count(1) from product_scorecard_intermediate_ewindow_hist

												 -- select * from rpt_product_scorecard_eWindow_solo_export
											 
												/* select enddate, cast( enddate as datetime2) , enddate
													 from rpt_ewindow_data 

													 SELECT [timestamp]  FROM rpt_ewindow_data WHERE ORDERID = 110580122 

													 SELECT TIMESTAMPDATE FROM product_scorecard_intermediate_ewindow_hist   WHERE ORDERID = 110580122 

													 SELECT [timestamp], timestamptime , format(  cast( timestamptime as datetime2) , 'hh:mm:ss tt' ) , 
													  concat( cast(  [timestamp] as date ), ' ', format(  cast( timestamptime as datetime2) , 'hh:mm:ss tt' ) )
													  FROM rpt_product_scorecard_eWindow_solo_export   WHERE ORDERID = 110580122 

													 -- format(  cast( [record closed timestamp] as datetime2) , 'hh:mm:ss tt' ) 

													 SELECT [TIMESTAMP] FROM  RPT_EWINDOW_DATA_HIST_bak WHERE ORDERID = 110580122 

													 select [TIMESTAMP] from network_pre_stg_platts_na_data_plattsnadatasheet where ORDERID = 110580122 


													 */ 

												  BEGIN TRY

															  insert into rpt_ewindow_data (             window_region                , timestamp			     ,  marketstate		    ,  orderid     
																									  ,  productname                  , hubname					 ,  stripname           ,  begindate
																									  ,  enddate                      , ordertype				 ,  price               ,  price_uom    
																									  ,  qtymultipliedout             , quantity				 ,  quantity_to         ,  units      
																									  ,  sendercompanyname            , orderstate				 ,  buyercompanyname    ,  sellercompanyname
																									  ,  orderclassification          , ocoorderids				 ,  market              ,  c1_percentage 											   
																									  ,  c1_pricing_basis             , c1_pricing_basis_period1 ,  c1_pricing_basis_period2      ,  c1_price      
																									  ,  c2_percentage                , c2_pricing_basis         ,  c2_pricing_basis_period1      ,  c2_pricing_basis_period2
																									  ,  c2_price                     , c3_percentage            ,  c3_pricing_basis              ,  c3_pricing_basis_period1  											     
																									  ,  c3_pricing_basis_period2     , c3_price                 ,  tqc                           ,  iscancelled      
																									  ,  Shell_Company              ,  timestampdate             , timestamptime
																									  ,  eventsequence )
																	   select        p.window_region   ,          cast(  p.[timestamp] as datetime ) [timestamp]
																	           ---   ,  concat( cast(  p.[timestamp] as date ), ' ', format(  cast( p.timestamptime as datetime2) , 'hh:mm:ss tt' ) )  
																			---       , ( case when p.[timestamp] is null then concat( cast(  p.[timestamp] as date ), ' ', format(  cast( p.timestamptime as datetime2) , 'hh:mm:ss tt' ) ) 
											                                 ---             else 	cast(  p.[timestamp] as datetime )	 end ) 		[timestamp]							        
																	              ,   p.marketstate        ,  p.orderid     
																				  ,  p.productname                  , p.hubname         ,  p.stripname           ,  cast( p.begindate as datetime2 ) as begindate 
																				  ,  cast( p.enddate as datetime2) as enddate                      , p.ordertype       ,  p.price               ,  p.price_uom    
																				  ,  cast(p.qtymultipliedout as float) as qtymultipliedout        ,   cast( p.quantity as float ) as  quantity   ,  p.quantity_to         ,  p.units          --- convert( real, p.quantity ) as  quantity
																				  ,  p.sendercompanyname            , p.orderstate      ,  p.buyercompanyname    ,  p.sellercompanyname  -- convert( real, p.qtymultipliedout) as qtymultipliedout       
																				  ,  p.orderclassification          , p.ocoorderids     ,  p.market              ,  p.c1_percentage  
																				  ,  p.c1_pricing_basis             , p.c1_pricing_basis_period1 ,  p.c1_pricing_basis_period2      ,  p.c1_price      
																				  ,  p.c2_percentage                , p.c2_pricing_basis         ,  p.c2_pricing_basis_period1      ,  p.c2_pricing_basis_period2
																				  ,  p.c2_price                     , p.c3_percentage            ,  p.c3_pricing_basis              ,  p.c3_pricing_basis_period1     
																				  ,  p.c3_pricing_basis_period2     , p.c3_price                 ,  p.tqc                           ,  p.iscancelled      
																				  ,  p.[Shell Company]                ,CAST (P.TIMESTAMPDATE AS DATETIME)  timestampdate            ,  p.timestamptime
																				  ,  min(p.eventsequence ) as min_event_sequence
																			  from network_pre_stg_platts_na_data_plattsnadatasheet p
																			  where  p.window_region= 'North America'
																			   group by 
																					 p. window_region                  , p.timestamp      ,  p.marketstate         ,  p.orderid     -- ,  p.EVENTSEQUENCE
																				  ,  p.productname                     , p.hubname         ,  p.stripname           ,  cast( p.begindate as datetime2 ) --p.begindate
																				  ,  cast( p.enddate as datetime2)     , p.ordertype       ,  p.price               ,  p.price_uom    
																				  ,  p.qtymultipliedout             , p.quantity        ,  p.quantity_to         ,  p.units      
																				  ,  p.sendercompanyname            , p.orderstate      ,  p.buyercompanyname    ,  p.sellercompanyname
																				  ,  p.orderclassification          , p.ocoorderids     ,  p.market              ,  p.c1_percentage  
																				  ,  p.c1_pricing_basis             , p.c1_pricing_basis_period1 ,  p.c1_pricing_basis_period2      ,  p.c1_price      
																				  ,  p.c2_percentage                , p.c2_pricing_basis         ,  p.c2_pricing_basis_period1      ,  p.c2_pricing_basis_period2
																				  ,  p.c2_price                     , p.c3_percentage            ,  p.c3_pricing_basis              ,  p.c3_pricing_basis_period1     
																				  ,  p.c3_pricing_basis_period2     , p.c3_price                 ,  p.tqc                           ,  p.iscancelled      
																				  ,  p.[Shell Company]                , p.timestampdate          ,  p.timestamptime 

													
																	  -- select * from rpt_ewindow_data

														END TRY

													  BEGIN CATCH
													   EXEC dbo.spErrorHandling 
													  END CATCH

										

								BEGIN TRY

								MERGE product_scorecard_intermediate_ewindow_hist t
								USING (   select   
												window_region                 , p.timestamp				 ,  p.marketstate				,  p.orderid     
											,  p.productname                  , p.hubname					 ,  p.stripname					,  p.begindate
											,  p.enddate                      , p.ordertype				 ,  p.price						,  p.price_uom    
											,  p.qtymultipliedout             , p.quantity				 ,  p.quantity_to				,  p.units      
											,  p.sendercompanyname            , p.orderstate				 ,  p.buyercompanyname			,  p.sellercompanyname
											,  p.orderclassification          , p.ocoorderids				 ,  p.market					,  p.c1_percentage  
											,  p.c1_pricing_basis             , p.c1_pricing_basis_period1 ,  p.c1_pricing_basis_period2      ,  p.c1_price      
											,  p.c2_percentage                , p.c2_pricing_basis         ,  p.c2_pricing_basis_period1      ,  p.c2_pricing_basis_period2
											,  p.c2_price                     , p.c3_percentage            ,  p.c3_pricing_basis              ,  p.c3_pricing_basis_period1     
											,  p.c3_pricing_basis_period2     , p.c3_price                 ,  p.tqc                           ,  p.iscancelled      
											,  p.Shell_Company                , p.timestampdate            ,  p.timestamptime
											,  p.eventsequence
									from (
										select            window_region                   , p.timestamp				 ,  p.marketstate					,  p.orderid     
														,  p.productname                  , p.hubname					 ,  p.stripname						,  p.begindate
														,  p.enddate                      , p.ordertype				 ,  p.price							,  p.price_uom    
														,  p.qtymultipliedout             , p.quantity				 ,  p.quantity_to					,  p.units      
														,  p.sendercompanyname            , p.orderstate				 ,  p.buyercompanyname				,  p.sellercompanyname
														,  p.orderclassification          , p.ocoorderids				 ,  p.market						,  p.c1_percentage  
														,  p.c1_pricing_basis             , p.c1_pricing_basis_period1 ,  p.c1_pricing_basis_period2      ,  p.c1_price      
														,  p.c2_percentage                , p.c2_pricing_basis         ,  p.c2_pricing_basis_period1      ,  p.c2_pricing_basis_period2
														,  p.c2_price                     , p.c3_percentage            ,  p.c3_pricing_basis              ,  p.c3_pricing_basis_period1     
														,  p.c3_pricing_basis_period2     , p.c3_price                 ,  p.tqc                           ,  p.iscancelled      
														,  p.Shell_Company                , CAST (P.TIMESTAMPDATE AS DATETIME)  timestampdate            ,  p.timestamptime
														,  p.eventsequence
														,  ( case when p.timestampdate <= @lvd_max_date_hist then 'EXCLUDE' ELSE 'KEEP' END ) rule_1
														-- ( case when p.timestampdate <= 'Jan 29 2021 12:00AM' then 'EXCLUDE' ELSE 'KEEP' END ) rule_1
												from rpt_ewindow_data p ) p
										where rule_1 = 'KEEP'
										AND timestamp is not null )  stg
                                        on ( 
												stg.window_region				= t.window_region				and stg.timestamp				= t.timestamp					and stg.marketstate				= t.marketstate					and stg.orderid						= t.orderid  and 
												stg.productname					= t.productname					and stg.hubname					= t.hubname						and stg.stripname				= t.stripname					and stg.begindate					= t.begindate  and 
												stg.enddate						= t.enddate						and stg.ordertype				= t.ordertype					and stg.price					= t.price						and stg.price_uom					= t.price_uom  and 
												stg.qtymultipliedout			= t.qtymultipliedout			and stg.quantity				= t.quantity					and stg.quantity_to				= t.quantity_to					and stg.units						= t.units  and 
												stg.sendercompanyname			= t.sendercompanyname			and stg.orderstate				= t.orderstate					and stg.buyercompanyname		= t.buyercompanyname			and stg.sellercompanyname			= t.sellercompanyname  and 
												stg.orderclassification			= t.orderclassification			and stg.ocoorderids				= t.ocoorderids					and stg.market					= t.market						and stg.c1_percentage				= t.c1_percentage  and 
												stg.c1_pricing_basis			= t.c1_pricing_basis			and stg.c1_pricing_basis_period1 = t.c1_pricing_basis_period1	and stg.c1_pricing_basis_period2 = t.c1_pricing_basis_period2   and stg.c1_price					= t.c1_price  and 
												stg.c2_percentage				= t.c2_percentage				and stg.c2_pricing_basis		= t.c2_pricing_basis			and stg.c2_pricing_basis_period1 = t.c2_pricing_basis_period1   and stg.c2_pricing_basis_period2	= t.c2_pricing_basis_period2  and 
												stg.c2_price					= t.c2_price					and stg.c3_percentage			= t.c3_percentage				and stg.c3_pricing_basis		 = t.c3_pricing_basis			and stg.c3_pricing_basis_period1	= t.c3_pricing_basis_period1  and 
												stg.c3_pricing_basis_period2	= t.c3_pricing_basis_period2	and stg.c3_price				= t.c3_price					and stg.tqc						 = t.tqc						and stg.iscancelled					= t.iscancelled  and 
												stg.Shell_Company				= t.Shell_Company				and stg.timestampdate			= t.timestampdate				and	 
												stg.eventsequence				= t.eventsequence		
											)
										when not matched then
										  insert ( window_region                , timestamp			     ,  marketstate		    ,  orderid     
																						  ,  productname                  , hubname					 ,  stripname           ,  begindate
																						  ,  enddate                      , ordertype				 ,  price               ,  price_uom    
																						  ,  qtymultipliedout             , quantity				 ,  quantity_to         ,  units      
																						  ,  sendercompanyname            , orderstate				 ,  buyercompanyname    ,  sellercompanyname
																						  ,  orderclassification          , ocoorderids				 ,  market              ,  c1_percentage 											   
																						  ,  c1_pricing_basis             , c1_pricing_basis_period1 ,  c1_pricing_basis_period2      ,  c1_price      
																						  ,  c2_percentage                , c2_pricing_basis         ,  c2_pricing_basis_period1      ,  c2_pricing_basis_period2
																						  ,  c2_price                     , c3_percentage            ,  c3_pricing_basis              ,  c3_pricing_basis_period1  											     
																						  ,  c3_pricing_basis_period2     , c3_price                 ,  tqc                           ,  iscancelled      
																						  ,  Shell_Company              , timestampdate              ,  timestamptime
																						  ,  eventsequence )
													  values (   stg.window_region                    , stg.timestamp				 ,  stg.marketstate          ,  stg.orderid     
															  ,  stg.productname                  , stg.hubname					 ,  stg.stripname           ,  stg.begindate
															  ,  stg.enddate                      , stg.ordertype				 ,  stg.price               ,  stg.price_uom    
															  ,  stg.qtymultipliedout             , stg.quantity				 ,  stg.quantity_to         ,  stg.units      
															  ,  stg.sendercompanyname            , stg.orderstate				 ,  stg.buyercompanyname    ,  stg.sellercompanyname
															  ,  stg.orderclassification          , stg.ocoorderids				 ,  stg.market              ,  stg.c1_percentage  
															  ,  stg.c1_pricing_basis             , stg.c1_pricing_basis_period1 ,  stg.c1_pricing_basis_period2      ,  stg.c1_price      
															  ,  stg.c2_percentage                , stg.c2_pricing_basis         ,  stg.c2_pricing_basis_period1      ,  stg.c2_pricing_basis_period2
															  ,  stg.c2_price                     , stg.c3_percentage            ,  stg.c3_pricing_basis              ,  stg.c3_pricing_basis_period1     
															  ,  stg.c3_pricing_basis_period2     , stg.c3_price                 ,  stg.tqc                           ,  stg.iscancelled      
															  ,  stg.Shell_Company                , stg.timestampdate            ,  stg.timestamptime
															  ,  stg.eventsequence );
											  

							END TRY

				BEGIN CATCH
					EXEC dbo.spErrorHandling 
				END CATCH


					IF EXISTS 
							(SELECT 
								TABLE_NAME 
							FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rpt_product_scorecard_eWindow_solo_export') 
		         
						drop table rpt_product_scorecard_eWindow_solo_export
										
     
					BEGIN TRY		         

				select * into rpt_product_scorecard_eWindow_solo_export from product_scorecard_intermediate_ewindow_hist

				END TRY

				BEGIN CATCH
					EXEC dbo.spErrorHandling 
				END CATCH



END 
 
 


GO



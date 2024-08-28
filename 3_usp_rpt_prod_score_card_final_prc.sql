USE [shell-01-eun-sqdb-koudqbyefeuwrybuauhd]
GO

/****** Object:  StoredProcedure [dbo].[usp_rpt_prod_scorecard_final]    Script Date: 8/26/2021 8:36:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





create  or alter      procedure [dbo].[usp_rpt_prod_scorecard_final]  as
    
	
	declare @lvd_max_date_hist          datetime
	declare @lvd_max_date               datetime


	declare @lvd_max_date_for_source    datetime


/*	-- =============================================
-- Author:						Vijaya Bhattaru

-- Description:					
-- input:						1.  product_scorecard_intermediate_ewindow_hist -- ( old one rpt_ewindow_data_hist ) 
                                2.ZEMA_PLATTS_COMBINED_VW 3. network_pre_stg_combined_pi_inputs_ewindowmappingsheet
-- output tables:				1.  product_scorecard_range_choice           2.product_scorecard_price			             3.product_scorecard_position2018	     4. product_scorecard_position
								5.  product_scorecard_ewindow_data           6. product_scorecard_ewindow_data2              7. product_scorecard_raw_data_step1     8. product_scorecard_raw_data_step2
								9.  product_scorecard_shell_active_records  10.  product_scorecard_non_shell_active_records 11. product_scorecard_non_active_records 12.product_scorecard_analysis_final
-- parameters:						 
-- Used in Front end reports: 
-- When should it run           The procedure has to run once after network file  gets loaded.
-- usage:                       exec [usp_rpt_prod_scorecard_final]
-- =============================================


select * from information_schema.columns where table_name = 'product_scorecard_position2018'

SELECT COUNT(1) FROM product_scorecard_range_choice  -- 476 - Spotfire: 477

-- parent price:  SELECT min(oprdate), max(oprdate)	FROM ZEMA_PLATTS_COMBINED_VW  -- 1,721,108

select min(date_chooser), max(date_chooser)  from product_scorecard_range_choice

select *  from product_scorecard_range_choice

select count(1) from product_scorecard_price  -- 1,770,465

select count(1) from product_scorecard_position2018  -- 137,937

select count(1) from product_scorecard_position   -- 137,937

select count(1) from product_scorecard_ewindow_data  -- 357,194

select * from product_scorecard_ewindow_data

select min ( cast( (timestampdate) as datetime) ) , max ( cast( (timestampdate) as datetime) ) from product_scorecard_intermediate_ewindow_hist

--1/10/2019 12:00:00 AM	Jan 29 2021 12:00AM

*/



BEGIN

/*
 
-- range_choice
	IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_range_choice' )

	   drop table product_scorecard_range_choice

	   -- select * from information_schema.columns where table_name = 'product_scorecard_range_choice'
	   -- select * from product_scorecard_range_choice

	   -- select count(1) from product_scorecard_range_choice

	     BEGIN TRY 

	 select 
			  cast (timestampdate as datetime)  as date_chooser, window_region as window_region_chooser, 
					( case when datediff( day, '01/01/2019', cast (timestampdate as datetime)  ) >= 0   or  datediff( day, '12/31/2022', cast (timestampdate as datetime) ) <= 0  then window_region else null end) window_region, 
					( case when datediff( day, '01/01/2019', cast (timestampdate as datetime)  ) >= 0   or  datediff( day, '12/31/2022', cast (timestampdate as datetime) ) <= 0  then cast (timestampdate as datetime) else null end ) date1
				--	( case when datediff( day, '01/01/2019',timestampdate   ) >= 0   or  datediff( day, '12/31/2022', timestampdate  ) <= 0  then window_region else null end) window_region, 
				--	( case when datediff( day, '01/01/2019', timestampdate  ) >= 0   or  datediff( day, '12/31/2022', timestampdate ) <= 0  then cast (timestampdate as datetime) else null end ) date1
			into product_scorecard_range_choice
             from product_scorecard_intermediate_ewindow_hist -- rpt_ewindow_data_hist
			 group by  TimeStampDate,   WINDOW_REGION
	   
		 END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH

	 
	 --- select count(1) from product_scorecard_price


	 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_price' )

	   drop table product_scorecard_price

	     BEGIN TRY 
	  
  select price_step3.average, price_step3.location, cast( price_step3.oprdate as date ) as dt, price_step3.plattscode, price_step3."Code 2" as code_2, price_step3.productname, price_step3.hubname,
      ---  p_price.average, price_step3.location, cast( price_step3.oprdate as date ) as dt, p_price.plattscode, price_step3."Code 2" as code_2, price_step3.productname, price_step3.hubname,
        (p_price.parent_price ) parent_price,
		  ( case when price_step3.location like '%USD/BBL%' then  price_step3.average + ISNULL(p_price.parent_price,0 )
	             when price_step3.location like '%USC/GAL%' then  ( ( price_step3.average + ISNULL(p_price.parent_price,0 ) ) / 100 ) *42
		   else null end ) as price,
		  ( case when price_step3.location like '%USD/BBL%' then  price_step3.average 
	                when price_step3.location like '%USC/GAL%' then  ( price_step3.average ) / 100*42
		   else null end )  as basis_price,
	    -- SUBSTRING(CONVERT(nvarchar(6),p_price.oprdate, 112),5,2) dateforfilter  -- extract month into two digit numbers
		CONVERT(nvarchar(6),cast( price_step3.oprdate as date ), 112)  dateforfilter
 	--	CONVERT(nvarchar(6),( .oprdate, 112)
    into product_scorecard_price
						  from 
						  	 (  select  v.average , v.oprdate, v.plattscode  , v.location,  n.productname, n.hubname, n.exp_index_nm, n.code, n."Code 2" , n.parent, n.publication, n."tick size" 
								 --n.code, n."code 2", n.parent,  n.publication
								   from  ( select * from ZEMA_PLATTS_COMBINED_VW v	where  oprdate < '2021-04-29 12:00:00.000'	) v						   
									  left outer join ( select   n.productname, n.hubname, n.exp_index_nm, n.code, n."Code 2" , n.parent, n.publication, n."tick size" 									                          
									                     from network_pre_stg_combined_pi_inputs_ewindowmappingsheet n
														  ) n
									 on v.plattscode = n."code" -- 1,818,025
									  ) price_step3
									 left outer join  
						 (  SELECT average as parent_price, oprdate, plattscode
										FROM ZEMA_PLATTS_COMBINED_VW 
										where oprdate < '2021-04-29 12:00:00.000') p_price	-- 1,787,253
						 on   p_price.oprdate         = price_step3.oprdate
						-- and  p_price.plattscode      = price_step3.plattscode
						   and  p_price.plattscode =      price_step3.[Code 2]

   END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH



----------------------------------


	IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_position2018' )

	   drop table product_scorecard_position2018

	  BEGIN TRY

		SELECT  ---top 100000  -- remove after fixing the voume issue
		 ew.window_region,  ew.first_flow, pos_hist.src_sys_nm ,day_of_week, commodity, group1 as [group],  exp_index_nm_analysis as exp_index_nm ,  exp_mo, inst_type_short_nm, hdr_fncl_phys_ind
	       ,ext_legal_entity_short_nm, int_bnes_unit_short_nm ,portfolio_nm ,  int_trdr_short_nm  ,unit      ,int_lentity_short_nm , total_position
		   ---cast(total_position as decimal(10,2)) total_position  -- vijaya
		into product_scorecard_position2018   	 
		FROM dbo.rpt_prod_position_solo_hist pos_hist,  
							( select     window_region, 
										 date1 first_flow,
										'EDS' as src_sys_nm
								from product_scorecard_range_choice  ) ew	
		where pos_hist.first_flow = ew.first_flow
		and ew.src_sys_nm = pos_hist.src_sys_nm
	
	 END TRY	 
 
 
   BEGIN CATCH
   EXEC dbo.spErrorHandling 
   END CATCH

   


-- POSITION product scorecard FINAL STEP


	IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_position' )

	   drop table product_scorecard_position

	  BEGIN TRY

  select window_region,  first_flow, src_sys_nm , day_of_week, commodity, [group],  exp_index_nm ,  exp_mo, inst_type_short_nm, hdr_fncl_phys_ind
	       ,ext_legal_entity_short_nm, int_bnes_unit_short_nm, portfolio_nm,  int_trdr_short_nm, unit, int_lentity_short_nm , 
		   --cast(total_position as decimal(10,2)) total_position,  -- vijaya
		   total_position,
		   phys_fin 
		into product_scorecard_position
	from (   select window_region,  first_flow, src_sys_nm , day_of_week, commodity, [group],  exp_index_nm ,  exp_mo, inst_type_short_nm, hdr_fncl_phys_ind
				   ,ext_legal_entity_short_nm, int_bnes_unit_short_nm, portfolio_nm,  int_trdr_short_nm, unit, int_lentity_short_nm ,total_position,
					( case when inst_type_short_nm like 'COMM%' then 'PHYSICAL'
						   when inst_type_short_nm like 'ENGY%' then 'FINANCIAL'
						   when inst_type_short_nm like 'OTC%' then 'FINANCIAL'
						   when inst_type_short_nm like 'CLEARED%' then 'FINANCIAL' 
					   ELSE 'BUILD RULE' end ) as phys_fin
			   from product_scorecard_position2018 ) position

 END TRY 
 
   BEGIN CATCH
   EXEC dbo.spErrorHandling 
   END CATCH



    /*   1. range_choice
        2. rpt_ewindow_data_hist 
		-- the below will create ewindow_data table in product_scorecard 
		select * from information_schema.columns where table_name = 'product_scorecard_ewindow_data'
	

		*/

		IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_ewindow_data' )

	   drop table product_scorecard_ewindow_data

	    BEGIN TRY

   
									select
												( case when shell_company = 'Yes' then qtymultipliedout else null end ) shell_volume,
												-- SUBSTRING(CONVERT(nvarchar(6),timestampdate, 112),5,2) dateforfilter , -- extract month into two digit numbers,
												-- CONVERT(nvarchar(6),timestampdate, 112) dateforfilter,
												dateforfilter,
												/* ( case when orderstate <> 'consummated' then 'EXCLUDE'
												when concat ( orderstate, shell_company ) = 'consummatedYes' then 'EXCLUDE'
												when ( productname like 'Platts Gasoline A%' or productname like 'Platts Gasoline M%' ) then substring( productname, 1, 17 )
												when productname = 'Platts Ethanol' then concat ( productname,'-', hubname )
												end ) graph_group, */
												( case when orderstate <> 'consummated' then 'EXCLUDE'
												when sum (( case when concat ( orderstate, shell_company ) = 'consummatedYes' then 1 else 0 end )) over (partition by hubname, productname, dateforfilter ) = 0 then 'EXCLUDE'
												when ( productname like 'Platts Gasoline A%' or productname like 'Platts Gasoline M%' ) then substring( productname, 1, 17 )
												when productname = 'Platts Ethanol' then concat ( productname,' ','-',' ', hubname )
												when ( productname not like 'Platts Gasoline A%' or productname not like 'Platts Gasoline M%' 
												or productname <> 'Platts Ethanol' )
												then  ( productname )
												end ) graph_group,
												CASE WHEN (
		SUM(CASE WHEN [ORDERSTATE] ='consummated' THEN 1 ELSE 0 END) OVER (PARTITION BY [PRODUCTNAME],[DATEFORFILTER]) >0
		AND
		SUM (CASE WHEN [shell_company] ='Yes' THEN 1 ELSE 0 END) OVER (PARTITION BY [PRODUCTNAME],[DATEFORFILTER])>0
		AND [ORDERSTATE] ='consummated'
		)
	THEN 'True'    ELSE 'False'
	END AS [FLAG],
												window_region, timestamp , marketstate , orderid
												, productname , hubname , stripname , begindate
												, enddate , ordertype , price , price_uom
												, qtymultipliedout , quantity , quantity_to , units, sendercompanyname
												, orderstate, buyercompanyname, sellercompanyname, orderclassification , ocoorderids , market , c1_percentage
												, c1_pricing_basis , c1_pricing_basis_period1 , c1_pricing_basis_period2 , c1_price
												, c2_percentage , c2_pricing_basis , c2_pricing_basis_period1 , c2_pricing_basis_period2
												, c2_price , c3_percentage , c3_pricing_basis , c3_pricing_basis_period1
												, c3_pricing_basis_period2 , c3_price , tqc , iscancelled, shell_company, timestampdate , timestamptime , eventsequence
												into product_scorecard_ewindow_data
												from
												(
												select range_choice.window_region
												, timestamp , marketstate , orderid
												, productname , hubname , stripname , begindate
												, enddate , ordertype ,  price    ---cast(price as decimal(10,2)) price  -- vijaya
												, price_uom
												, qtymultipliedout, quantity, quantity_to , units
												, rtrim( replace ( sendercompanyname,',','' ) ) sendercompanyname
												, orderstate
												, rtrim( replace ( buyercompanyname,',','' ) ) buyercompanyname
												, rtrim( replace ( sellercompanyname,',','' ) ) sellercompanyname
												, orderclassification , ocoorderids , market , c1_percentage
												, c1_pricing_basis , c1_pricing_basis_period1 , c1_pricing_basis_period2 , c1_price
												, c2_percentage , c2_pricing_basis , c2_pricing_basis_period1 , c2_pricing_basis_period2
												, c2_price , c3_percentage , c3_pricing_basis , c3_pricing_basis_period1
												, c3_pricing_basis_period2 , c3_price , tqc , iscancelled
												, ( case when shell_company is null then 'no' else 'Yes' end ) shell_company
												, CONVERT(nvarchar(6),cast( timestampdate as date ), 112) dateforfilter
												, timestampdate , cast(timestamptime as time)timestamptime , eventsequence
												--- ( Case when [TimeStampTime]>Time(${GraphTimeSelector}) then null else [TimeStampTime] end -- change this after you get clarification from Mrunali.
												-- TimeStampTime as graph_time
												from product_scorecard_intermediate_ewindow_hist ew_hist, ( select date1 as date_chooser, window_region as window_region_chooser,
												window_region,
												date1
												from product_scorecard_range_choice ) range_choice
												where ew_hist.window_region = range_choice.window_region
												and ew_hist.timestampdate = range_choice.date_chooser ) a
		
		
		-- select shell_company from product_scorecard_intermediate_ewindow_hist
		
			END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH



		   	-----------
			-- ewindow data2 -- 
			-- select * from dbo.product_scorecard_ewindow_data
			--------------
			-- SELECT * FROM [product_scorecard_ewindow_data2] WHERE ORDERID = 110580122

		IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_ewindow_data2' )

	   drop table product_scorecard_ewindow_data2

	    BEGIN TRY
        
		 select window_region, timestampdate, [timestamp], marketstate  ,orderid  ,productname  ,hubname  ,stripname  ,begindate  ,enddate  
		       ,ordertype, price, price_uom  ,qtymultipliedout  ,quantity  ,quantity_to  ,units  ,sendercompanyname  ,orderstate  ,buyercompanyname  
			   ,sellercompanyname  ,orderclassification  ,ocoorderids  ,market  ,c1_percentage  ,c1_pricing_basis  ,c1_pricing_basis_period1  ,c1_pricing_basis_period2	  ,c1_price  ,c2_percentage  
			   ,c2_pricing_basis  ,c2_pricing_basis_period1  ,c2_pricing_basis_period2  ,c2_price  ,c3_percentage  ,c3_pricing_basis  ,c3_pricing_basis_period1  ,c3_pricing_basis_period2  ,c3_price ,tqc  
			   ,iscancelled  ,shell_company, timestamptime  ,eventsequence, shell_volume, dateforfilter, graph_group, shell_direction, [FLAG]
			   --, graph_time,	   'company_color' as company_color -- this transfomration is missing. Ask Mrunali.
		into product_scorecard_ewindow_data2
		from ( 
		SELECT shell_volume ,dateforfilter  ,graph_group  ,window_region  , [timestamp] ,marketstate  ,orderid  ,productname  ,hubname  ,stripname
			  ,begindate  ,enddate  ,ordertype  ,price_uom  ,qtymultipliedout  ,quantity  ,quantity_to  ,units  ,sendercompanyname
			  ,orderstate  ,buyercompanyname  ,sellercompanyname  ,orderclassification  ,ocoorderids  ,market  ,c1_percentage  ,c1_pricing_basis  ,c1_pricing_basis_period1  ,c1_pricing_basis_period2
			  ,c1_price  ,c2_percentage  ,c2_pricing_basis  ,c2_pricing_basis_period1  ,c2_pricing_basis_period2  ,c2_price  ,c3_percentage  ,c3_pricing_basis  ,c3_pricing_basis_period1  ,c3_pricing_basis_period2
			  ,c3_price  ,tqc  ,iscancelled  ,shell_company  ,timestampdate  ,timestamptime  ,  eventsequence,
			  isnull(price ,0) price, ( case when buyercompanyname is null then 'NON SHELL'
                                             when buyercompanyname = 'Shell Trading US Company' then 'BUY'
                                             when SELLERCOMPANYNAME = 'Shell Trading US Company' then 'SELL'
                                             else 'NON SHELL' end) shell_direction, [FLAG]
         FROM dbo.product_scorecard_ewindow_data ) ewindow_data

		 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		   -------------------------------
		   --- raw data step 1
-- inputs: 
--1. select top 100 *  from product_scorecard_ewindow_data
--2. select * from network_pre_stg_combined_pi_inputs_ewindowmappingsheet

-- select * from   from product_scorecard_ewindow_data

---------------------------------------------------------------

			IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_raw_data_step1' )

	   drop table product_scorecard_raw_data_step1

	   -- SELECT * FROM [product_scorecard_raw_data_step1] WHERE ORDERID = 110580122
	 

	    BEGIN TRY 


		 select window_region, cast( timestampdate as date ) as timestampdate, [timestamp], marketstate  ,orderid, stripname, cast( begindate as date ) as begindate, cast ( enddate as date) as enddate, ordertype,
				 price as base_price, price_uom as base_price_uom, QTYMULTIPLIEDOUT as base_qtymultipliedout, quantity as base_quantity, units as base_units,
				 sendercompanyname  ,orderstate  ,buyercompanyname  ,sellercompanyname  ,orderclassification  ,ocoorderids  ,market , shell_company, timestamptime ,
				--- RANK() OVER  (PARTITION BY orderid , timestampdate order by eventsequence) eventsequence,   --- vijaya
				 RANK() OVER  (PARTITION BY orderid , timestampdate order by cast( eventsequence as numeric ) ,  orderid,  timestampdate  ) eventsequence,
				-----eventsequence,   
				 cast( ( case when price is null then 0   when price_uom = 'USD / bbl' then  cast( price as real ) when price_uom = 'USD / gal' then cast( price as real ) *42
						 when price_uom = 'USD / mt'  then cast( price as real )  / 8.45	 else null 		 end ) as 
						 --decimal(10,2))price, 
						 float  )price,  -- vijaya
				( Case  when price_uom in ('USD / gal', 'USD / mt', 'USD / bbl') then 'USD / bbl' else null end  ) price_uom,
				  ( case when units in ('bbl') then  convert (numeric, QTYMULTIPLIEDOUT )
						when units in ('gal') then  convert (numeric, QTYMULTIPLIEDOUT ) / 42 
						when units in ('mt')  then  convert (numeric, QTYMULTIPLIEDOUT ) * 8.45
				   else null end )  QTYMULTIPLIEDOUT,
				( case when units in ('bbl') then  convert (numeric, quantity )
						when units in ('gal') then  convert (numeric, quantity ) / 42 
						when units in ('mt')  then  convert (numeric, quantity ) * 8.45
				   else null end ) quantity,
			   ( case when units in ('bbl') then  'bbl'
						when units in ('gal') then 'bbl'
						when units in ('mt')  then  'bbl'
				   else null end )  units,
				mapping.exp_index_nm, mapping.code, mapping.[code 2], mapping.parent, mapping.[tick size], mapping.publication     
		  into product_scorecard_raw_data_step1
		  from product_scorecard_ewindow_data  ew_data
		   left outer join
			( select  PRODUCTNAME,            HUBNAME,            EXP_INDEX_NM,            Code,           [Code 2],             PARENT,           [Tick Size],            [PUBLICATION]
			   from network_pre_stg_combined_pi_inputs_ewindowmappingsheet ) mapping
		 on     ew_data.productname = mapping.productname
			and ew_data.hubname     = mapping.hubname
    

	 END TRY 

 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH


		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_raw_data_step2' )

	   drop table product_scorecard_raw_data_step2

	   -- SELECT * FROM product_scorecard_raw_data_step2 WHERE ORDERID = 110580122 ORDER BY [record closed timestamptime]
	   

	   

	    BEGIN TRY
		

			SELECT 
			 [window_region]      ,  [date]     ,[timestamp]      ,[marketstate]      ,[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
			  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
			  ,[orderclassification]      ,[ocoorderids]      ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
			  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication],
			  [record closed timestamp], [Record Closed TimeStampDate],
			  --cast( [record closed timestamp] as date) as [Record Closed TimeStampDate],
              --- format(  cast( [record closed timestamp] as datetime2) , 'hh:mm:ss tt' )  [record closed timestamptime]
			  [record closed timestamptime]
			into product_scorecard_raw_data_step2
			from (
					SELECT [window_region]      , [timestampdate] as [date]     ,[timestamp]      ,[marketstate]      ,[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
						  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
						  ,[orderclassification]      ,[ocoorderids]      ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
						  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], 
						   [record closed timestamp],
						   (  Case  when [record closed timestampdate] is null then  cast( [record closed timestamp] as date) 
								   else [Record Closed TimeStampDate] end ) [Record Closed TimeStampDate],
								--  ( case when [record closed timestamptime] is null then   format(  cast( [record closed timestamp] as datetime2) , 'hh:mm:ss tt' )  else [record closed timestamptime] end )  [record closed timestamptime]
								[record closed timestamptime]
											from  (		
								SELECT [window_region]      , step1.[timestampdate]      ,[timestamp]      ,[marketstate]      ,step1.[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
									  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
									  ,[orderclassification]      ,[ocoorderids]      ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
									  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication],  
									 (  Case  when ([record closed timestamp] is null) and ([ORDERSTATE] in ('consummated', 'inactive', 'withdrawn')) then [timestamp]
											  when ([record closed timestamp] is null) and ([ORDERSTATE] in ('active')) then  concat (step1.timestampdate, ' 11:59:59 PM' )  ---step1.[timestamp] ---concat (step1.timestampdate, ' 11:59:59 PM' ) 
											   else [Record Closed TIMESTAMP] end )  [record closed timestamp], --,
									 ( case when [record closed timestamptime] is null then   format(  cast( [record closed timestamp] as datetime2) , 'hh:mm:ss tt' )  else [record closed timestamptime] end )  [record closed timestamptime],
									 ( case when [record closed timestampdate] is null then  cast( [record closed timestamp] as date) else [record closed timestampdate] end ) [record closed timestampdate]					
									from  [dbo].[product_scorecard_raw_data_step1] step1
										   left outer join
											( select orderid, next_eventsequence, timestampdate, [record closed timestamp], [record closed timestamptime], [record closed timestampdate]
												from 
											 ( select orderid, 
											    --- ( RANK() OVER  (PARTITION BY orderid , timestampdate order by eventsequence)  - 1 ) next_eventsequence,  --vijaya
												     (  RANK() OVER  (PARTITION BY orderid , timestampdate order by eventsequence,  orderid,  timestampdate  )   - 1 ) next_eventsequence,
													  timestampdate, timestamp [record closed timestamp], timestamptime [record closed timestamptime],
													  timestampdate [record closed timestampdate]
												from product_scorecard_raw_data_step1 )  a
												group by orderid, next_eventsequence, timestampdate, [record closed timestamp], [record closed timestamptime], [record closed timestampdate] ) step2_1
										 on     step1.[timestampdate] = step2_1.[timestampdate]
											and step1.orderid     = step2_1.orderid
											and step1.[eventsequence] = step2_1.next_eventsequence 
								) a ) b
    

	 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		  

IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_shell_active_records' )

	   drop table product_scorecard_shell_active_records

	  

	    BEGIN TRY

		select * 
		into product_scorecard_shell_active_records
		from (
		SELECT [window_region]      ,  [date]     ,[timestamp]      ,[marketstate]      ,[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
			  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
			  ,[orderclassification]      ,[ocoorderids]      ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
			  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp],
			   [Record Closed TimeStampDate], [record closed timestamptime],
			 --  ( case when [orderstate] in ( 'active') and [shell_company] ='Shell Company' then 'KEEP' else 'DELETE' end ) [rule],
			  ( case when orderstate = 'active' and [shell_company] = 'Yes' then 'KEEP' ELSE 'DELETE' END ) [RULE] ,
			   [timestamp] shell_active_timestamp, [record closed timestamp] shell_close_timestamp, [price] shell_price, [orderid] shell_orderid, [eventsequence] shell_eventsequence
		  from product_scorecard_raw_data_step2 
		   ) a
		where [rule] = 'KEEP'
    
	-- select count(1) from product_scorecard_shell_active_records

	 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH


		  

 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_non_shell_active_records_pre' )

	   drop table product_scorecard_non_shell_active_records_pre

	   --- select count(1) from product_scorecard_non_shell_active_records_pre

	    BEGIN TRY

		select [window_region]      ,  [date]     ,[timestamp]      ,[marketstate]       ,[orderid]      ,[stripname]          ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
			  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]   ,[sendercompanyname]  ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]	  ,[orderclassification]      ,[ocoorderids]    
			  ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]			  ,[units]      ,[exp_index_nm]   
			  ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp], [record closed timestamptime],   [Record Closed TimeStampDate],	    
			  --  [rule],
			 shell_active_timestamp, shell_close_timestamp, shell_orderid, shell_eventsequence, shell_price
			 /*  ( case  when shell_active_timestamp is  null and maxbid_minoffer2 is not null and  [ordertype]='Offer' 
                                                         then min([price]) over  (partition by  [ordertype], market,  shell_active_timestamp ) 
			           when shell_active_timestamp is  null and maxbid_minoffer2 is not null and  [ordertype]='Bid' 
                                                         then max([price]) over  (partition by  [ordertype], market,  shell_active_timestamp )  
                  else maxbid_minoffer2 end ) maxbid_minoffer */
	  into product_scorecard_non_shell_active_records_pre
		from (
		select [window_region]      ,  [date]     ,[timestamp]      ,[marketstate]       ,[orderid]      ,[stripname]          ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
			  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]   ,[sendercompanyname]  ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]	  ,[orderclassification]      ,[ocoorderids]    
			  ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]			  ,[units]      ,[exp_index_nm]   
			  ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp], [record closed timestamptime],   [Record Closed TimeStampDate],	    
			  --  [rule],
			 shell_active_timestamp,
			 ( case  when shell_active_timestamp is null then null else shell_close_timestamp end ) shell_close_timestamp,
			 ( case  when shell_active_timestamp is null then null else shell_orderid end ) shell_orderid,
			 ( case  when shell_active_timestamp is null then null else shell_eventsequence end ) shell_eventsequence,
			 ( case  when shell_active_timestamp is null then null else shell_price end ) shell_price
			/* ( case when [ordertype]='Bid' then max([price]) over (partition by market,shell_active_timestamp) 
                    when [ordertype]='Offer' then min([price]) over  (partition by market,shell_active_timestamp) else NULL end ) maxbid_minoffer2	*/	  -- VIJAYA
 --- into product_scorecard_non_shell_active_records_pre_step
			from (
			select [window_region]      ,  [date]     ,[timestamp]      ,[marketstate]       ,[orderid]      ,[stripname]          ,[begindate]      ,[enddate]      ,[ordertype]      ,[base_price]
						  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]   ,[sendercompanyname]  ,[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]	  ,[orderclassification]      ,[ocoorderids]    
						  ,[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]			  ,[units]      ,[exp_index_nm]   
						  ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp], [record closed timestamptime],   [Record Closed TimeStampDate],	    
						  --  [rule],
						 ( case  when [timestamp]>=shell_close_timestamp then null when [record closed timestamp]<=shell_active_timestamp then null
								 else shell_active_timestamp end ) shell_active_timestamp,
						  shell_close_timestamp,
						  shell_orderid,
						  shell_eventsequence,
						  shell_price
						 --	, ( case when [ordertype]='Bid' then max([price]) over (partition by market,shell_active_timestamp) 
						  --          when [ordertype]='Offer' then min([price]) over  (partition by market,shell_active_timestamp) else NULL end ) maxbid_minoffer				 
					from( 
						SELECT [window_region]      ,  step2.[date]     ,[timestamp]      ,[marketstate]      ,[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,step2.[ordertype]      ,[base_price]
						  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,step2.[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
						  ,[orderclassification]      ,[ocoorderids]      ,step2.[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
						  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp], [record closed timestamptime],   [Record Closed TimeStampDate],
						  shell.shell_close_timestamp, shell.shell_active_timestamp, shell.shell_orderid, shell.shell_eventsequence, shell.shell_price,
						--  ( Case  when shell.shell_active_timestamp is null then 'KEEP'
						--          when datediff(minute,shell.shell_active_timestamp,[record closed timestamp])<0 then 'DELETE' else 'KEEP' end ) [rule]
						 ( Case  when shell.shell_active_timestamp is null then 'KEEP'
								--  when shell.shell_active_timestamp is not null  and 
							--	when  convert( integer, datediff(minute,shell.shell_active_timestamp,[record closed timestamp]) ) < 0 then 'DELETE'  -- vijaya
							---	when  convert( numeric, datediff(minute,[record closed timestamp], shell.shell_active_timestamp   ) ) < 0 then 'DELETE' -- 400,237
							--when   datediff(minute,shell.shell_active_timestamp,[record closed timestamp])  < 0 then 'DELETE' 
							when  cast(  datediff(ss,shell_active_timestamp,	[record closed timestamp] )/cast(60 as decimal(4,2) ) as real )  < 0 then 'DELETE' 
								 else 'KEEP' end ) [rule]
					  from (  		select * from  
					( SELECT [window_region]      ,  step2.[date]     ,[timestamp]      ,[marketstate]      ,[orderid]      ,[stripname]      ,[begindate]      ,[enddate]      ,step2.[ordertype]      ,[base_price]
						  ,[base_price_uom]      ,[base_qtymultipliedout]      ,[base_quantity]      ,[base_units]      ,[sendercompanyname]      ,step2.[orderstate]      ,[buyercompanyname]      ,[sellercompanyname]
						  ,[orderclassification]      ,[ocoorderids]      ,step2.[market]      ,[shell_company]      ,[timestamptime]      ,[eventsequence]      ,[price]      ,[price_uom]      ,[QTYMULTIPLIEDOUT]      ,[quantity]
						  ,[units]      ,[exp_index_nm]      ,[code]      ,[code 2]      ,[parent]      ,[tick size]      ,[publication], [record closed timestamp], [record closed timestamptime],   [Record Closed TimeStampDate],
						 ( case when orderstate = 'active' and [shell_company] = 'No' then 'KEEP' ELSE 'DELETE' END ) [RULE]              
					  from product_scorecard_raw_data_step2 step2 ) step2    -- 357,194
				  WHERE step2.[RULE] = 'KEEP' ) step2  -- 297682
					  left outer join
					  ( select shell_close_timestamp, shell_active_timestamp, [market], [ordertype], [orderstate], [date], shell_orderid, shell_eventsequence, shell_price
						  from   product_scorecard_shell_active_records
						  group by  shell_close_timestamp, shell_active_timestamp, [market], [ordertype], [orderstate], [date], shell_orderid, shell_eventsequence, shell_price 
						  ) shell
					 on step2.[ordertype]  = shell.[ordertype]
					and step2.[orderstate] = shell.[orderstate]
					and step2.[market]     = shell.[market]
					and step2.[date]       = shell.[date] 		
					 ) step5
					 where [rule] = 'KEEP') a ) b

		 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		   IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_non_shell_active_records' )

	   drop table product_scorecard_non_shell_active_records

	   --- select count(1) from product_scorecard_non_shell_active_records

	    BEGIN TRY

      select * 
	  into product_scorecard_non_shell_active_records
	  from  (
		select 
		    a.*,
		 ( case when [ordertype]='Bid' then max([price]) over (partition by [ordertype], market,shell_active_timestamp order by ord_no  )
			    when [ordertype]='Offer' then min([price]) over  (partition by [ordertype], market,shell_active_timestamp order by ord_no   ) else NULL end ) maxbid_minoffer	
		from
		( select s.*,
		       ( case when [ordertype]='Bid' then 1
			          when [ordertype]='Offer' then 2 
					else 3  end) ord_no
				  from product_scorecard_non_shell_active_records_pre s ) a ) b 
		---where orderid = 930939758

		 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

	


IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_non_active_records' )

	   drop table product_scorecard_non_active_records

	   -- select count(1) from product_scorecard_non_active_records

	    BEGIN TRY

select 
     [window_region]          ,[date]                       ,[timestamp]        ,[marketstate]   ,[orderid]                ,[stripname]      ,[begindate]        ,[enddate]                 , [ordertype]              ,[base_price]
	,[base_price_uom]         ,[base_qtymultipliedout]      ,[base_quantity]    ,[base_units]    ,[sendercompanyname]      ,[orderstate]     ,[buyercompanyname] ,[sellercompanyname]       ,[orderclassification]     ,[ocoorderids] 
	,[market]                 ,[shell_company]              ,[timestamptime]    ,[eventsequence] ,[price]                  ,[price_uom]      ,[QTYMULTIPLIEDOUT] ,[quantity]                ,[exp_index_nm]            ,[code]  
	,[code 2]                 ,[parent]                     ,[tick size]        ,[publication]   ,[record closed timestamp], [record closed timestamptime]	,[Record Closed TimeStampDate]  , units
 into product_scorecard_non_active_records
  FROM 
	( 
select    
	 [window_region]          ,[date]                       ,[timestamp]        ,[marketstate]   ,[orderid]                ,[stripname]      ,[begindate]        ,[enddate]                 , [ordertype]              ,[base_price]
	,[base_price_uom]         ,[base_qtymultipliedout]      ,[base_quantity]    ,[base_units]    ,[sendercompanyname]      ,[orderstate]     ,[buyercompanyname] ,[sellercompanyname]       ,[orderclassification]     ,[ocoorderids] 
	,[market]                 ,[shell_company]              ,[timestamptime]    ,[eventsequence] ,[price]                  ,[price_uom]      ,[QTYMULTIPLIEDOUT] ,[quantity]                ,[exp_index_nm]            ,[code]  
	,[code 2]                 ,[parent]                     ,[tick size]        ,[publication]   ,[record closed timestamp], [record closed timestamptime]	,[Record Closed TimeStampDate],	units,
	( case when [orderstate] not in ('active' ) then 'KEEP' else 'DELETE' end ) [rule]
from product_scorecard_raw_data_step2 step2 
  ) raw_data_step2
where [rule] = 'KEEP'
    

	 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH


		   -------------------

		   IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step4' )

	   drop table product_scorecard_analysis_step4

	   -- select count(1) from product_scorecard_analysis_step4  -- 696,468  -- 695,371

	    BEGIN TRY

		 select * 
		into product_scorecard_analysis_step4
				from  (
							select   window_region			,date						,timestamp     ,marketstate    ,orderid           ,stripname		,begindate        ,enddate						  ,ordertype					 ,base_price
									,base_price_uom			,base_qtymultipliedout		,base_quantity ,base_units     ,sendercompanyname ,orderstate		,buyercompanyname ,sellercompanyname			  ,orderclassification			 ,ocoorderids
									,market					,shell_company				,timestamptime ,eventsequence  ,price             ,price_uom		,QTYMULTIPLIEDOUT ,quantity						  ,units						 ,exp_index_nm
									,code					,[code 2]					,parent        ,[tick size]      ,publication       ,[record closed timestamp]       ,[Record Closed TimeStampDate]  ,[record closed timestamptime] ,shell_active_timestamp
									,shell_close_timestamp  ,shell_price,shell_orderid  ,shell_eventsequence, maxbid_minoffer
								from product_scorecard_non_shell_active_records n
								union all
							select  window_region			,date						,timestamp     ,marketstate    ,orderid           ,stripname		,begindate        ,enddate						  ,ordertype					 ,base_price
									,base_price_uom			,base_qtymultipliedout		,base_quantity ,base_units     ,sendercompanyname ,orderstate		,buyercompanyname ,sellercompanyname			  ,orderclassification			 ,ocoorderids
									,market					,shell_company				,timestamptime ,eventsequence  ,price             ,price_uom		,QTYMULTIPLIEDOUT ,quantity						  ,units						 ,exp_index_nm
									,code					,[code 2]					,parent        ,[tick size]      ,publication       ,[record closed timestamp]       ,[Record Closed TimeStampDate]  ,[record closed timestamptime] ,shell_active_timestamp
									,shell_close_timestamp  ,shell_price,shell_orderid  ,shell_eventsequence, null maxbid_minoffer
								from product_scorecard_shell_active_records a
								union all
								select  window_region		,date					    ,timestamp     ,marketstate    ,orderid           ,stripname		,begindate        ,enddate						  ,ordertype					 ,base_price
									,base_price_uom			,base_qtymultipliedout		,base_quantity ,base_units     ,sendercompanyname ,orderstate		,buyercompanyname ,sellercompanyname			  ,orderclassification			 ,ocoorderids
									,market					,shell_company				,timestamptime ,eventsequence  ,price             ,price_uom		,QTYMULTIPLIEDOUT ,quantity						  ,units						 ,exp_index_nm
									,code					,[code 2]					,parent        ,[tick size]      ,publication       ,[record closed timestamp]       ,[Record Closed TimeStampDate]  ,[record closed timestamptime] 
									,null shell_active_timestamp        ,null shell_close_timestamp  ,null shell_price, null shell_orderid  , null shell_eventsequence, null maxbid_minoffer
								from product_scorecard_non_active_records na 
								) step4	
    

	 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

--------------------

   

		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step6' )

	   drop table product_scorecard_analysis_step6

	   -- select count(1) from product_scorecard_analysis_step6  --  696,468

	    BEGIN TRY

				select * 
				into product_scorecard_analysis_step6
						 from  ( 
						SELECT STEP4.*, positions.position FROM product_scorecard_analysis_step4 STEP4
  							left outer join 
																(  select exp_index_nm, cast( first_flow as date) [date], sum(total_position) position
																	from product_scorecard_position							
																  group by exp_index_nm, cast( first_flow as date) ) positions				
													on  step4.exp_index_nm = positions.exp_index_nm
													and step4.[date]       = positions.[date] ) a	
    

	   END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

---------------------------------


		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step8' )

	   drop table product_scorecard_analysis_step8

	   -- select count(1) from product_scorecard_analysis_step8  --  847575

	    BEGIN TRY

				select * 
				into product_scorecard_analysis_step8
						 from  ( 
						SELECT step6.*, price.productname, hubname, market_price, dateforfilter  dateforfilter2
						  FROM product_scorecard_analysis_step6 step6
  								left outer join
										( select productname, hubname, basis_price market_price, dateforfilter, plattscode code, dt [date]
											from product_scorecard_price ) price
							on step6.code    = price.code
							and step6.[date] = price.[date] ) a
    

	   END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		   --------------------------------------------------

		   

		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step10' )

	   drop table product_scorecard_analysis_step10

	   -- select count(1) from product_scorecard_analysis_step10  -- 847,575

	    BEGIN TRY

				select * 
				into product_scorecard_analysis_step10
				from 
				 ( 	SELECT step8.*,  [Potential Weight for P1],  [Potential Weight for P10] , [Potential Weight for P11],  [Potential Weight for P12] , [Potential Weight for P2]
																	  ,[Potential Weight for P3],  [Potential Weight for P4],  [Potential Weight for P5],  [Potential Weight for P6] 
																	   ,[Potential Weight for P7],[Potential Weight for P8],  [Potential Weight for P9]
						  FROM product_scorecard_analysis_step8 step8
  									left outer join 
									( select * from 
											(
												select window_region,  convert( real, [potential weight] ) [potential weight],  concat('Potential Weight for ',[code] ) [code]
												from network_pre_stg_combined_pi_inputs_scorechartsheet 
  														) a
														pivot 
														(
														sum ( [potential weight] ) 
															for [code] in (   [Potential Weight for P1],  [Potential Weight for P10] , [Potential Weight for P11],  [Potential Weight for P12] , [Potential Weight for P2]
																			  ,[Potential Weight for P3],  [Potential Weight for P4],  [Potential Weight for P5],  [Potential Weight for P6],  [Potential Weight for P7]
																			  ,[Potential Weight for P8],  [Potential Weight for P9] )
											)p  ) step10_score_chart
										on step8.window_region =  step10_score_chart.window_region    ) a
    

	   END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		   --------------------------------------------------

		   
		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step11' )

	   drop table product_scorecard_analysis_step11

	   -- select count(1) from product_scorecard_analysis_step11  --  847,575

	   

	    BEGIN TRY

				select * 
				into product_scorecard_analysis_step11
				from 
				 ( 	SELECT step10.*,  step11.notes
						  FROM product_scorecard_analysis_step10 step10
  									left outer join
								( select [date], market, notes
									from network_pre_stg_product_scorecard_comments_sheet1sheet) step11
								on step10.[date] = step11.[date]
							and  step10.[market] = step11.[market]    ) a
    

	   END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH



		   --------------------------------------------------

		      
		  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_step12_11' )

	   drop table product_scorecard_analysis_step12_11

	  /* select timestamp, shell_vwap, market_vwap, shell_deal_consummated, market , s.eventsequence, s.shell_eventsequence, s.*
	       from product_scorecard_analysis_step12_11 s where orderid = 897311075  order by s.eventsequence  -- 847,575
		   go
		   select a.timestamp, a.shell_vwap, a.market_vwap, a.shell_deal_consummated, a.market , a.eventsequence, a.shell_eventsequence, a.*
	       from rpt_analysis_data a where orderid = 897311075  order by a.eventsequence

	  -- select  RANK() OVER  (PARTITION BY orderid  order by cast( [timestamp] as datetime),  orderstate  ) eventsequence
	    from product_scorecard_analysis_step12_11 where orderid = 897311075  --order by a.eventsequence
	  */

	    BEGIN TRY

				select * 
				into product_scorecard_analysis_step12_11
				from  ( select a.* , 
				                 ( case when shell_deal_absolute_consumated_volume is null then null
						   when sellercompanyname = 'Shell Trading US Company' then - [shell_deal_absolute_consumated_volume] 
						   when buyercompanyname = 'Shell Trading US Company' then  [shell_deal_absolute_consumated_volume] end ) shell_deal_consummated
				      from  
				      (select    window_region		,date					,timestamp		,marketstate		,orderid           
								 ,stripname		    ,begindate               ,enddate		,ordertype			,base_price
								 ,base_price_uom		,base_qtymultipliedout	,base_quantity	,base_units		,sendercompanyname 
								 ,orderstate		    ,buyercompanyname         ,sellercompanyname,orderclassification	,ocoorderids
								 ,market				,shell_company			
							     ,eventsequence
								 ,price				,price_uom	
								 ,QTYMULTIPLIEDOUT	,quantity					,units			,exp_index_nm			 ,code		
								 ,[code 2]			,parent					,[tick size]		,publication			,[record closed timestamp] 
								 ,[Record Closed TimeStampDate]  ,[record closed timestamptime] ,shell_active_timestamp, shell_close_timestamp, shell_price,shell_orderid  
								 ,shell_eventsequence, timestamptime
								 ,position, productname, hubname, market_price, dateforfilter2,
							   [Potential Weight for P1],  [Potential Weight for P10] , [Potential Weight for P11],  [Potential Weight for P12] , 
							   [Potential Weight for P2],  [Potential Weight for P3],  [Potential Weight for P4],  [Potential Weight for P5],  
							   [Potential Weight for P6],  [Potential Weight for P7],  [Potential Weight for P8],  [Potential Weight for P9],
							   notes,
							   ( case when shell_company='No' then null when orderstate <>'consummated' then NULL else QTYMULTIPLIEDOUT end ) shell_deal_absolute_consumated_volume,
							  cast( ( case when shell_company<>'Yes' then NULL when orderstate<>'consummated' then NULL 
	  								  else Sum(QTYMULTIPLIEDOUT * price) over (PARTITION BY shell_company,orderstate, date,market   )
								   / Sum(QTYMULTIPLIEDOUT) over (PARTITION BY shell_company,orderstate, date,market   ) end )as float ) shell_vwap,  -- decimal(10,4)
							 cast( ( case  when orderstate <>'consummated'  then NULL
	  								  else Sum(QTYMULTIPLIEDOUT * price) over (PARTITION BY orderstate, date,market   )
								   / Sum(QTYMULTIPLIEDOUT) over (PARTITION BY orderstate, date,market   )end )as float) market_vwap,
							  cast(( case  when orderstate <>'consummated'  then NULL  when shell_company = 'Yes' then null
	  								  else Sum(QTYMULTIPLIEDOUT * price) over (PARTITION BY shell_company,orderstate, date,market   )
								   / Sum(QTYMULTIPLIEDOUT) over (PARTITION BY shell_company,orderstate, date,market   )end )as float) market_vwap_less_shell	
								 , maxbid_minoffer	
									  FROM product_scorecard_analysis_step11 step11
  								  ) a ) b

								 
    

	   END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH

		   --------------------------------------------------

		   -- 

	*/	   


	--- update product_scorecard_analysis_step12_11 set market_vwap_less_shell = 0 where market_vwap_less_shell is null
	 
	

		      -- product_scorecard_analysis_pre_final1

			IF EXISTS 
					  (SELECT 
						 TABLE_NAME 
					 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_pre_final1' )

				   drop table product_scorecard_analysis_pre_final1

					BEGIN TRY


			select max( pre_factor_for_p1) over (PARTITION BY date,market ) as factor_for_p1, 
				   max( pre_factor_for_p2) over (PARTITION BY date,market ) as factor_for_p2,
				   max( pre_factor_for_p3) over (PARTITION BY date,market  ) as factor_for_p3,
				   ( case  when [ordertype] = 'Offer' then 0 
						   when open_at_close_window = 'False' then 0
						   when max([PRICE]) over (PARTITION BY [ordertype], open_at_close_window, date, market ) = [PRICE] and  [shell_company] = 'Yes' then 1 else 0 end )
					 pre_factor_for_p4,
				   ( case  when [ordertype] = 'Bid' then 0 
						   when open_at_close_window = 'False' then 0
						   when min([PRICE]) over (PARTITION BY  [ordertype], open_at_close_window, date, market ) = [PRICE] and  [shell_company] = 'Yes' then 1 else 0 end )
					 pre_factor_for_p5,	
					 ( case when  [rank_in_shell_active_window] = 1 and  
						 sum(  case when shell_company = 'Yes' then 1 else -5000 end ) over (PARTITION BY  [shell_orderid],[shell_eventsequence] , [ordertype], [rank_in_shell_active_window]) > 0 
						  then [PRICE] else null end ) shell_high_low_bid_offer_for_range,
					  /* spotfire transformation: UniqueConcatenate([Shell Company]) over ([SHELL ORDERID],[SHELL EVENTSEQUENCE],[ORDERTYPE],[RANK IN SHELL ACTIVE WINDOW]) in ("Yes"))
					  -- I had to convert shell company value from string to integer to make use of partition  */	
					(  case when orderstate <>'consummated'  then 0
							when avg(shell_buy_vwap) over (PARTITION BY  market, date  ) is null then 0
							when avg(shell_buy_vwap) over (PARTITION BY  market, date  ) is not null and  avg(market_vwap_less_shell) over (PARTITION BY  market, date  ) is null then 1
							when avg(shell_buy_vwap) over (PARTITION BY  market, date  ) <= avg(market_vwap_less_shell) over (PARTITION BY  market, date  ) then 0
							when avg(shell_buy_vwap) over (PARTITION BY  market, date  ) > avg(market_vwap_less_shell) over (PARTITION BY  market, date ) then 			 
								( avg(shell_buy_vwap) over (PARTITION BY  market, date  ) - avg(market_vwap_less_shell) over (PARTITION BY  market, date  ) )  * 10000 / 25 / 10000 * 25 / [Tick Size]
								   end )  pre_factor_for_p8,			
					(  case when orderstate <>'consummated'  then 0
							when avg(shell_sell_vwap) over (PARTITION BY  market, date  ) is null then 0
							when avg(shell_sell_vwap) over (PARTITION BY  market, date  ) is not null and 
							 avg(market_vwap_less_shell) over (PARTITION BY market,date  ) is null then 1			   
							when avg(shell_sell_vwap) over (PARTITION BY  market, date  ) 
							>= avg(market_vwap_less_shell) over (PARTITION BY  market,date  ) 
							then 0
							when avg(shell_sell_vwap) over (PARTITION BY  market, date ) < avg(market_vwap_less_shell) over (PARTITION BY  market,date ) then 			 
							 ( avg(shell_sell_vwap) over (PARTITION BY  market, date ) - avg(market_vwap_less_shell) over (PARTITION BY  market,date ) )  * 10000 / 25 / 10000 * 25 / [Tick Size]
								   end )  pre_factor_for_p9,	
				  ( case when shell_percent_consummated is null then 0
						 when shell_percent_consummated < 0.5 then 0
						 when shell_percent_consummated >= 0.5 and shell_percent_consummated < 0.75 then 1
						 when shell_percent_consummated >=0.75 and shell_percent_consummated < 1 then 2
						 when shell_percent_consummated = 1 then 3 end ) factor_for_p10,
					0 factor_for_p12,
				 ( case when shell_percent_consummated is null then 0 else  sum (shell_deal_consummated) over  (PARTITION BY date,market ) end ) shell_net_consummated,
				a.*
				into product_scorecard_analysis_pre_final1
				from (
				select 	
					  ( case when orderstate <>'consummated'  then 0
					  ---(PARTITION BY orderstate, market, date  order by ord_rank asc ) 
									when sum (position) over (PARTITION BY  market, date  ) is null then 0
									when sum (position) over  (PARTITION BY  market, date  ) = 0  then 0
									when Sum(shell_vwap) over (PARTITION BY  market, date  ) is null then 0
									when Sum(market_vwap) over  (PARTITION BY  market, date  ) = 0 then 0
									when avg(shell_vwap) over  (PARTITION BY  market, date  )
									 =  avg(market_vwap_less_shell) over  (PARTITION BY  market, date  ) then 0 
									when sum(market_vwap_less_shell) over  (PARTITION BY  market, date  )
									 is null and shell_vwap is not null then 1
									--- when sum(market_vwap_less_shell) over  (PARTITION BY  market, date  )
									-- = 0 and shell_vwap <> 0 then 1  -- new one
									when  
										avg(position) over  (PARTITION BY  market, date  ) > 0 
								and sum(shell_deal_consummated) over  (PARTITION BY  market, date  ) < 0 
								and avg(shell_vwap) over  (PARTITION BY  market, date  )
									< avg(market_vwap_less_shell) over  (PARTITION BY  market, date  ) then 0	 
							when  
										avg(position) over  (PARTITION BY  market, date  ) < 0 
								and sum(shell_deal_consummated) over  (PARTITION BY  market, date  ) > 0 
								and avg(shell_vwap) over  (PARTITION BY  market, date  )
									> avg(market_vwap_less_shell) over  (PARTITION BY  market, date  ) then 0	
							when  
										avg(position) over ( PARTITION BY  market, date ) > 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) > 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
									< avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0   
							when  
										avg(position) over ( PARTITION BY  market, date ) < 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) < 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
									> avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0   
							when  
										avg(position) over ( PARTITION BY  market, date ) > 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) < 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
									> avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0 
							when  
										avg(position) over ( PARTITION BY  market, date ) < 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) > 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
									< avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0 
							when  
										avg(position) over ( PARTITION BY  market, date ) < 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) > 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
									> avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0 
							when  
										avg(position) over ( PARTITION BY  market, date ) > 0 
								and sum(shell_deal_consummated) over ( PARTITION BY  market, date ) < 0 
								and avg(shell_vwap) over ( PARTITION BY  market, date )
								< avg(market_vwap_less_shell) over ( PARTITION BY  market, date ) then 0 
							when  
											avg(position) over  (PARTITION BY  market, date  ) < 0 
									and sum(shell_deal_consummated) over  (PARTITION BY  market, date  ) < 0 
									and avg(shell_vwap) over  (PARTITION BY  market, date  )
										<   avg(  market_vwap_less_shell  )
										 over  (PARTITION BY  market, date  )
										then 1 
										---COALESCE(col1, 0)
							when  
											avg(position) over  (PARTITION BY  market, date  ) > 0 
									and sum(shell_deal_consummated) over  (PARTITION BY  market, date  ) > 0 
									and avg(shell_vwap) over  (PARTITION BY  market, date  )
										> avg(market_vwap_less_shell) over  (PARTITION BY  market, date  ) then 1 
							else null
									end ) pre_factor_for_p1, 
							( case when orderstate <> 'active' then 0 
									when [ordertype] = 'Offer' then 0 
									when [shell_company] = 'No' then 0
									-- over (PARTITION BY market, date order by date)='No' then 0  -- not working
									when max([PRICE]) over ( PARTITION BY  market, date ) <> [shell_price] then 0 
									when max([PRICE]) over ( PARTITION BY  market, date ) = [shell_price] then 1 
									when [shell_company]='No' then 0  else null end ) pre_factor_for_p2,
							( case when orderstate <> 'active' then 0 
									when [shell_company] = 'No' then 0
									when [ordertype] = 'Bid' then 0
									-- over (PARTITION BY market, date order by date)='No' then 0  -- not working
									when min([PRICE]) over ( PARTITION BY  market, date ) <> [shell_price] then 0 
									when min([PRICE]) over ( PARTITION BY  market, date ) = [shell_price] then 1 
									when [shell_company]='No' then 0  else null end ) pre_factor_for_p3,
							(  case when orderstate <> 'active' then 'False'
									when  [record closed timestamp] >=  concat( date, ' 7:29:30 PM' ) and [timestamp] <= concat( date, ' 7:29:30 PM' )  THEN 'True' 
										else 'False' end ) open_at_close_window,
							( case when orderstate <> 'active' then null
									when [shell_active_timestamp] is null then null 
									when [ordertype] = 'Offer' then rank() over ( PARTITION BY   [shell_orderid], [shell_eventsequence], [date] 
									order by [PRICE], [shell_orderid], [shell_eventsequence], [date]  )
									when [ordertype] = 'Bid' then rank() over ( PARTITION BY  [shell_orderid], [shell_eventsequence], [date] 
									order by  [PRICE] desc, [shell_orderid], [shell_eventsequence], [date] ) end )   [rank_in_shell_active_window],
							cast(( case when  [shell_company] <> 'Yes' then null
									when orderstate <> 'consummated' then null               
									when [ordertype] <>  'Offer' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by [shell_company], orderstate, [ordertype], [date], [market] )  
									/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by [shell_company], orderstate, [ordertype], [date], [market] )  end )as float) shell_buy_vwap,	
							--	(case  when [ordertype] = 'Bid' then rank() over ( PARTITION BY [PRICE], [shell_orderid], [shell_eventsequence], [date] order by [PRICE], [shell_orderid], [shell_eventsequence], [date] desc) end )   [rank_in_shell_active_window],
							cast(( case when  [shell_company] <> 'Yes' then null
									when orderstate <> 'consummated' then null               
									when [ordertype] <>  'Bid' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by [shell_company], orderstate, [ordertype], [date], [market]  )  
									/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by [shell_company], orderstate, [ordertype], [date], [market]  )   end )as float) shell_sell_vwap,  
							cast(( case 
									when orderstate <> 'consummated' then null               
									when [ordertype] <>  'Offer' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by orderstate, [ordertype], [date], [market] )  
									/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by orderstate, [ordertype], [date], [market]  ) end )as float) market_buy_vwap,    
								cast(( case 
									when orderstate <> 'consummated' then null       
									when [ordertype] <>  'Bid' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by  orderstate, [ordertype], [date], [market]   )  
									/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by orderstate, [ordertype], [date], [market] ) end )as float) market_sellvwap, 
							cast(( case when  [shell_company] = 'No' then [price] else null end )as float) non_shell_price,
							cast(( case when orderstate <> 'consummated' then null 
									when  [shell_company] = 'Yes' then null                 
									when [ordertype] <>  'Bid' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by orderstate, [shell_company], [ordertype], [date], [market]  ) 
									/ Sum([QTYMULTIPLIEDOUT] ) over  ( partition by orderstate, [shell_company], [ordertype], [date], [market]  ) end )as float) market_less_shell_buy_vwap,
							cast(( case when orderstate <> 'consummated' then null 
									when  [shell_company] = 'Yes' then null                 
									when [ordertype] <>  'Offer' then null
								else Sum([QTYMULTIPLIEDOUT] * [PRICE]) over ( partition by orderstate, [shell_company], [ordertype], [date], [market])  
									/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by orderstate, [shell_company], [ordertype], [date], [market]  ) end )as float) market_less_shell_sell_vwap,							  
								( case 
									when orderstate <> 'consummated' then null  
									when sum( shell_deal_absolute_consumated_volume)  over ( partition by orderstate, [date], [market]  ) is null then null
										else sum ( abs ( shell_deal_absolute_consumated_volume) ) over (  partition by orderstate, [date], [market] ) 
										/ Sum([QTYMULTIPLIEDOUT] ) over ( partition by orderstate, [date], [market] )
									end
									) shell_percent_consummated,
							( case when   ( case when
								sum(  case when lower(trim(shell_company)) = 'yes' then 1 else 0 end ) 
								over (PARTITION BY  [date], [market] ) > 0  then 1 else 0 end )  = 1 then 'True' else 'False' end ) shell_active_flag,
								CONVERT(nvarchar(6),[date], 112) as  dateforfilter,  -- spotfire:      Expression: Concatenate(String(DatePart("year",[DATE])), Case  when Integer(DatePart("mm",[DATE]))<10 then Concatenate("0",String(Integer(DatePart("mm",[DATE])))) else String(Integer(DatePart("mm",[DATE]))) end)
							( case when orderstate = 'consummated' and [shell_company] = 'Yes' and max([timestamp]) 
								over  (PARTITION BY  orderstate,[shell_company], [date], [market] order by  [date] ) <> [timestamp] then 0 
									-- when orderstate = 'consummated' and [shell_company] = 'Yes' and max([timestamp]) over  (PARTITION BY  orderstate,[date], [market] order by  [date] ) = [timestamp] then 1 else 0 end ) last_consummated_flag,
									when orderstate = 'consummated' and [shell_company] = 'Yes' and rank() over  (PARTITION BY  orderstate, [shell_company], [date], [market] order by  [date] ) = 1 
									then 1 else 0 end ) last_consummated_flag,
									-- last_consummated_flag has to be revisited once we compare these data tables with spotfire data tables 
step12_11.*	  
from 
( select b.*, ( case when orderstate = 'consummated' then 1 else 4 end) ord_rank
from product_scorecard_analysis_step12_11 b  ) step12_11 			  
) a


/*select p.*,
( case when orderstate = 'consummated' then 1 else 4 end) ord_rank
		 from 
	   product_scorecard_analysis_step12_11 p */


				 END TRY 
 
					   BEGIN CATCH
					   EXEC dbo.spErrorHandling 
					   END CATCH
		
	
IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_pre_final2' )

	   drop table product_scorecard_analysis_pre_final2

	    BEGIN TRY


						SELECT [factor_for_p1]	  ,[factor_for_p2]  ,[factor_for_p3]	 -- ,[pre_factor_for_p4]
							 , max( pre_factor_for_p4) over (PARTITION BY market, date ) [factor_for_p4]
							 -- ,[pre_factor_for_p5]
							 , max( pre_factor_for_p5) over (PARTITION BY market, date ) [factor_for_p5]
							  ,[shell_high_low_bid_offer_for_range]
							  , (  case when orderstate <> 'active' then null
										when [shell_active_timestamp] is null then null
										when count([shell_high_low_bid_offer_for_range])  over ( PARTITION BY  [shell_orderid]  , [shell_eventsequence], [date]  ) = 0 then null
										when [ordertype] = 'Bid' and rank() over ( partition by [shell_orderid], [shell_eventsequence], [date] order by  price desc ) = 2 then price
										when  [ordertype] = 'Offer' and rank() over ( partition by [shell_orderid], [shell_eventsequence], [date] order by  price ) = 2 then price end 
								 ) [next_high_in_shell_group]
							  --,[pre_factor_for_p8]
							   , abs( pre_factor_for_p8) [factor_for_p8]
							 -- ,[pre_factor_for_p9]
							  ,abs( pre_factor_for_p9) [factor_for_p9]  ,[factor_for_p10]	 ,[factor_for_p12]	 ,[shell_net_consummated]   
							  ,[open_at_close_window] ,[rank_in_shell_active_window]  ,[shell_buy_vwap] ,[shell_sell_vwap]  ,[market_buy_vwap]  ,[market_sellvwap]
							  ,[non_shell_price] ,[market_less_shell_buy_vwap] ,[market_less_shell_sell_vwap],[shell_percent_consummated]  ,[shell_active_flag]
							  ,[dateforfilter2]  ,[last_consummated_flag]  ,[window_region]  ,[date]	  ,[timestamp]	  ,[marketstate]  ,[orderid]
							  ,[stripname]	  ,[begindate]	  ,[enddate]  ,[ordertype]	  ,[base_price]  ,[base_price_uom]
							  ,[base_qtymultipliedout]  ,[base_quantity] ,[base_units]  ,[sendercompanyname]  ,[orderstate]
							  ,[buyercompanyname]	  ,[sellercompanyname] ,[orderclassification]  ,[ocoorderids]	  ,[market]
							  ,[shell_company]  ,[eventsequence] ,[price]  ,[price_uom]	  ,[QTYMULTIPLIEDOUT]  ,[quantity]
							  ,[units]  ,[exp_index_nm]  ,[code]  ,[code 2]  ,[parent] ,[tick size]  ,[publication]  ,[record closed timestamp]
							  ,[Record Closed TimeStampDate]  ,[record closed timestamptime]	  ,[shell_active_timestamp]  ,[shell_close_timestamp]
							  ,[shell_price]  ,[shell_orderid]  ,[shell_eventsequence]	  ,[position]	  ,[productname]  ,[hubname]
							  ,[market_price]  ,[dateforfilter]	  ,[Potential Weight for P1]	  ,[Potential Weight for P10]
							  ,[Potential Weight for P11]  ,[Potential Weight for P12] ,[Potential Weight for P2]  ,[Potential Weight for P3]	  ,[Potential Weight for P4]
							  ,[Potential Weight for P5] ,[Potential Weight for P6] ,[Potential Weight for P7] ,[Potential Weight for P8]	  ,[Potential Weight for P9]
							  ,[notes]	  ,[shell_deal_absolute_consumated_volume]  ,[shell_vwap] ,[market_vwap]  ,[market_vwap_less_shell]  ,[shell_deal_consummated]
							  ,[timestamptime], maxbid_minoffer
						   into [product_scorecard_analysis_pre_final2]
						  FROM [dbo].product_scorecard_analysis_pre_final1

							 END TRY 
 
								   BEGIN CATCH
								   EXEC dbo.spErrorHandling 
								   END CATCH


IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_analysis_final' )

	   drop table product_scorecard_analysis_final

	    BEGIN TRY


select [factor_for_p1]					,[factor_for_p2]					,[factor_for_p3]				,[factor_for_p4]			,[factor_for_p5]                ,[factor_for_p6]			,[factor_for_p7]						,[factor_for_p8]			,[factor_for_p9]        ,[factor_for_p10]
       ,[factor_for_p11]			    ,[factor_for_p12]					,[shell_net_consummated]		,[open_at_close_window]		,[rank_in_shell_active_window]  ,[shell_buy_vwap]			,[shell_sell_vwap]						,[market_buy_vwap]			,[market_sellvwap]      ,[non_shell_price]
      ,[market_less_shell_buy_vwap]		,[market_less_shell_sell_vwap]  ,[shell_percent_consummated]    ,[shell_active_flag]		,[dateforfilter2]				 ,[last_consummated_flag]   ,[window_region]						,[date]						,[timestamp]			,[marketstate]	
	  ,[orderid]						,[stripname]						,[begindate]					,[enddate]					,[ordertype]					 ,[base_price]				,[base_price_uom]						,[base_qtymultipliedout]   ,[base_quantity]			,[base_units]    
	  ,[sendercompanyname]				,[orderstate]						,[buyercompanyname]				,[sellercompanyname]		,[orderclassification]			 ,[ocoorderids]				,[market]								,[shell_company]			,[eventsequence]		,[price]
      ,[price_uom]						,[QTYMULTIPLIEDOUT]					,[quantity]						,[units]					,[exp_index_nm]					 ,[code]					,[code 2]								,[parent]					,[tick size]			,[publication]
      ,[record closed timestamp]		,[Record Closed TimeStampDate]      ,[record closed timestamptime]  ,[shell_active_timestamp]	,[shell_close_timestamp]		 ,[shell_price]				,[shell_orderid]						,[shell_eventsequence]     ,[position]				,[productname]
      ,[hubname]  						,[market_price]					    ,[dateforfilter]				,[Potential Weight for P1]  ,[Potential Weight for P10]      ,[Potential Weight for P11] ,[Potential Weight for P12]			,[Potential Weight for P2] ,[Potential Weight for P3] ,[Potential Weight for P4]
      ,[Potential Weight for P5]		,[Potential Weight for P6]      ,[Potential Weight for P7]			,[Potential Weight for P8]  ,[Potential Weight for P9]       ,[notes]					,[shell_deal_absolute_consumated_volume] ,[shell_vwap]				,[market_vwap]			,[market_vwap_less_shell]
      ,[shell_deal_consummated]			,[next_high_in_shell_group]     ,score, [shell_high_low_bid_offer_for_range],
	   ( case when sum(score)  over ( PARTITION BY [DATE],[MARKET]) = 0 then 'False' else 'True' end ) shell_score_flag,[timestamptime], maxbid_minoffer,
	   ( case when score >= 60 then 'True' else 'False' end ) score_min -- check with Mrunali
	into product_scorecard_analysis_final
			from (
			select [factor_for_p1]				,[factor_for_p2]					,[factor_for_p3]				,[factor_for_p4]			,[factor_for_p5]               , [factor_for_p6]			,[factor_for_p7]   ,[factor_for_p8]			, [factor_for_p9]      ,[factor_for_p10]
				   ,[factor_for_p11]			,[factor_for_p12]					,[shell_net_consummated]		,[open_at_close_window]		,[rank_in_shell_active_window]  ,[shell_buy_vwap]			,[shell_sell_vwap]  ,[market_buy_vwap]			,[market_sellvwap]      ,[non_shell_price]
				  ,[market_less_shell_buy_vwap] ,[market_less_shell_sell_vwap]  ,[shell_percent_consummated]    ,[shell_active_flag]		,[dateforfilter2]				 ,[last_consummated_flag]   ,[window_region]    ,[date]						,[timestamp]			,[marketstate]	
				  ,[orderid]					,[stripname]						,[begindate]					,[enddate]					,[ordertype]					 ,[base_price]				,[base_price_uom]   ,[base_qtymultipliedout]    ,[base_quantity]		,[base_units]    
				  ,[sendercompanyname]			,[orderstate]						,[buyercompanyname]				,[sellercompanyname]		,[orderclassification]			 ,[ocoorderids]				,[market]			,[shell_company]			,[eventsequence]		,[price]
				  ,[price_uom]					,[QTYMULTIPLIEDOUT]					,[quantity]						,[units]					,[exp_index_nm]					 ,[code]					,[code 2]			,[parent]					,[tick size]			,[publication]
				  ,[record closed timestamp]    ,[Record Closed TimeStampDate]      ,[record closed timestamptime]  ,[shell_active_timestamp]	,[shell_close_timestamp]		 ,[shell_price]				,[shell_orderid]    ,[shell_eventsequence]      ,[position]				,[productname]
				  ,[hubname]  			        ,[market_price]					    ,[dateforfilter]				,[Potential Weight for P1]  ,[Potential Weight for P10]      ,[Potential Weight for P11] ,[Potential Weight for P12] ,[Potential Weight for P2] ,[Potential Weight for P3] ,[Potential Weight for P4]
				  ,[Potential Weight for P5]      ,[Potential Weight for P6]      ,[Potential Weight for P7]      ,[Potential Weight for P8]      ,[Potential Weight for P9]      ,[notes]      ,[shell_deal_absolute_consumated_volume]      ,[shell_vwap]      ,[market_vwap]      ,[market_vwap_less_shell]
				  ,[shell_deal_consummated],   [next_high_in_shell_group], [shell_high_low_bid_offer_for_range], [timestamptime],
								   (( [Potential Weight for P1] * [factor_for_p1]) + ( [Potential Weight for P2] * [factor_for_p2]) + ( [Potential Weight for P3] * [factor_for_p3]) + ( [Potential Weight for P4] * [factor_for_p4]) + ( [Potential Weight for P5] * [factor_for_p5] ) +
								   (( [Potential Weight for P6] * Max([factor_for_p6]) over ( PARTITION BY [DATE],[MARKET])) + 
								   ( [Potential Weight for P7] * Max([factor_for_p7])  over ( PARTITION BY [DATE],[MARKET])) + 
								   ( [Potential Weight for P8] * Max([factor_for_p8])  over ( PARTITION BY [DATE],[MARKET])) + 
								   ( [Potential Weight for P9] * Max([factor_for_p9])  over ( PARTITION BY [DATE],[MARKET])) + 
								   ( [Potential Weight for P10] * Max([factor_for_p10]) over ( PARTITION BY [DATE],[MARKET])) +
								   ( [Potential Weight for P11] * [factor_for_p11]) + ([Potential Weight for P12] * Max([factor_for_p12])  over ( PARTITION BY [DATE],[MARKET]))) ) score, maxbid_minoffer
			from (
			SELECT [factor_for_p1]				  ,[factor_for_p2]				  ,[factor_for_p3]				  ,[factor_for_p4]				  ,[factor_for_p5]
				  , ( case  when [ordertype] <> 'Bid' then 0 
							when orderstate <> 'active' then 0
							when count( [shell_high_low_bid_offer_for_range] ) over  ( PARTITION BY [date], [shell_orderid]  , [shell_eventsequence]  ) = 0 then 0
							when  avg([shell_high_low_bid_offer_for_range]) over  ( PARTITION BY [date], [shell_orderid]  , [shell_eventsequence]  ) is not null 
								  and avg([next_high_in_shell_group]) over  ( PARTITION BY [date], [shell_orderid]  , [shell_eventsequence]  )  is null  then 0
								  else ( avg([shell_high_low_bid_offer_for_range]) over  ( PARTITION BY [date], [shell_orderid]  , [shell_eventsequence]  ) - 
										 avg([next_high_in_shell_group]) over  ( PARTITION BY [date], [shell_orderid]  , [shell_eventsequence]  )/[Tick Size] ) end
						   ) [factor_for_p6], [timestamptime]
				  ,  ( case  when [ordertype] <> 'Offer' then 0 
							when orderstate <> 'active' then 0
							when count( [shell_high_low_bid_offer_for_range] ) over  ( PARTITION BY [ordertype], orderstate, [date], [shell_orderid]  , [shell_eventsequence] ) = 0 then 0
							when  avg([shell_high_low_bid_offer_for_range]) over  ( PARTITION BY [ordertype], orderstate, [date], [shell_orderid]  , [shell_eventsequence]  ) is not null 
								  and avg([next_high_in_shell_group]) over  ( PARTITION BY [ordertype], orderstate, [date], [shell_orderid]  , [shell_eventsequence]  )  is null  then 0
								  else ( avg([shell_high_low_bid_offer_for_range]) over  ( PARTITION BY [ordertype], orderstate, [date], [shell_orderid]  , [shell_eventsequence]  ) - 
										 avg([next_high_in_shell_group]) over  ( PARTITION BY [ordertype], orderstate, [date], [shell_orderid]  , [shell_eventsequence])/[Tick Size] ) end
						   ) [factor_for_p7]
				  ,[shell_high_low_bid_offer_for_range]				  ,[next_high_in_shell_group]				  ,[factor_for_p8]				  ,[factor_for_p9]
				  ,[factor_for_p10]
				  				  ,( case when [factor_for_p10] = 3 then 0 else  max( [last_consummated_flag]) over (PARTITION BY  date, market )  end ) [factor_for_p11],[factor_for_p12]
				  ,[shell_net_consummated]			,[open_at_close_window]			  ,[rank_in_shell_active_window]	,[shell_buy_vwap]
				  ,[shell_sell_vwap]				,[market_buy_vwap]				  ,[market_sellvwap]				,[non_shell_price]
				  ,[market_less_shell_buy_vwap]	    ,[market_less_shell_sell_vwap]	  ,[shell_percent_consummated]		,[shell_active_flag]
				  ,[dateforfilter2]   			    ,[last_consummated_flag]		  ,[window_region]  				,[date]
				  ,[timestamp]     				    ,[marketstate] 				      ,[orderid] 				        ,[stripname]
				  ,[begindate]						,[enddate]						  ,[ordertype]		        	    ,[base_price]
				  ,[base_price_uom] 				,[base_qtymultipliedout]		  ,[base_quantity]       		    ,[base_units]
				  ,[sendercompanyname]	            ,[orderstate] 					  ,[buyercompanyname]	            ,[sellercompanyname]
				  ,[orderclassification]			,[ocoorderids]					  ,[market]							,[shell_company]
				  ,[eventsequence]					,[price]						  ,[price_uom]						,[QTYMULTIPLIEDOUT]
				  ,[quantity]						,[units]						  ,[exp_index_nm]					,[code]
				  ,[code 2]							,[parent]						  ,[tick size]						,[publication]
				  ,[record closed timestamp]		,[Record Closed TimeStampDate]	  ,[record closed timestamptime]    ,[shell_active_timestamp]
				  ,[shell_close_timestamp]			,[shell_price]					  ,[shell_orderid]					,[shell_eventsequence]
				  ,[position]						,[productname]					  ,[hubname]						,[market_price]
				  ,[dateforfilter]					,[Potential Weight for P1]		  ,[Potential Weight for P10]		,[Potential Weight for P11]
				  ,[Potential Weight for P12]		,[Potential Weight for P2]		  ,[Potential Weight for P3]		,[Potential Weight for P4]
				  ,[Potential Weight for P5]		,[Potential Weight for P6]		  ,[Potential Weight for P7]		,[Potential Weight for P8]
				  ,[Potential Weight for P9]		,[notes]						  ,[shell_deal_absolute_consumated_volume],[shell_vwap]
				  ,[market_vwap] 					,[market_vwap_less_shell]		  ,[shell_deal_consummated]	, maxbid_minoffer			  
			  FROM [dbo].[product_scorecard_analysis_pre_final2]  )pre_final ) a

	 END TRY 
 
		   BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		   END CATCH


		   

END
	
	
 









GO



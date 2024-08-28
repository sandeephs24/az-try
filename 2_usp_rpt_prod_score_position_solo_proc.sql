USE [shell-01-eun-sqdb-koudqbyefeuwrybuauhd]
GO

/****** Object:  StoredProcedure [dbo].[usp_rpt_prod_score_position_solo]    Script Date: 7/10/2021 8:50:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



create  or alter   procedure [dbo].[usp_rpt_prod_score_position_solo]  as
    
	
	declare @lvd_max_date_hist          date
	declare @lvd_min_date_hist          date
	declare @lvd_max_date               datetime
	declare @lvd_max_date_for_source    datetime


	-- =============================================
-- Author:						Vijaya Bhattaru

-- Description:					
-- input:						1. NETWORK_PRE_STG_PRODUCTS_MAPPING_SHEET1SHEET   2.  ddf_non_power_positions -- where EXP_INDEX_NM LIKE 'PLT_%' ESCAPE '\' 
-- output tables:				1.  2.  3. rpt_prod_position_solo_hist (appends data)
-- parameters:						None
-- Used in Front end reports: 
-- When should it run           The procedure has to run once after network file plattsnadata gets loaded.
-- usage:                       exec usp_rpt_prod_score_ewindow_solo @table_name = 'rpt_ewindow_data'
-- =============================================

/*
-- select count(1) from NETWORK_PRE_STG_PRODUCTS_MAPPING_SHEET1SHEET  -- 925 rows same as Spotfire
   select count(1) from rpt_prod_position_solo_hist           -- 236,369 same as spotfire


   -- tables list:
   select count(1) from rpt_gas_crude_products_step1  -- 7047 -- MATCHED WITH Spotfire
    select * from rpt_gas_crude_products_step1
	select * from information_schema.columns where table_name = 'intermediate_gas_crdue_step2_final' order by column_name

	select distinct count(deal_track_no) from intermediate_gas_crdue_step2_final

	delete from  rpt_prod_position_solo_hist where first_flow >= cast ( '2021-02-27' as date  )

 delete from rpt_prod_position_solo_hist where first_flow is null



   select count(1) from intermediate_gas_crdue_step2_1

   select count(1) from rpt_gas_crude_products_step1

   select count(1) from rpt_gas_crude_products_step1

   dodd frank querry NON power position -- 
     select exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm
			 cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,                int_trdr_long_nm,     int_trdr_short_nm
			 proc_eod_dt, tr_leg_fncl_phys_ind,   deal_undisc_delta_qty
		 from  ddf_non_power_positions
		 where  src_sys_nm = 'EDS'
		  AND proc_eod_dt > '2021-02-26'
		   and  EXP_INDEX_NM LIKE 'PLT_%' ESCAPE '\'

	select count(1) from rpt_gas_crude_products_pre_step1	
			 
			   
--	select count(1) from rpt_prod_position_solo_hist where first_flow < cast ( '2021-02-27' as date  )  -- 229,496
select * into rpt_prod_position_solo_hist_bak from  rpt_prod_position_solo_hist 

---delete from  rpt_prod_position_solo_hist where first_flow >= cast ( '2021-02-27' as date  )

select count(1) from rpt_prod_position_solo_hist where first_flow is not null  -- 229,496

delete from rpt_prod_position_solo_hist where first_flow is null

truncate table rpt_prod_position_solo_hist

select count(1) from rpt_prod_position_solo_hist  

	select distinct  eds_flow_date_range
	  from rpt_prod_position_solo_hist h
	  where first_flow <> eds_flow_date_range


*/
BEGIN 

 -- gas crude products step1

 -- uncomment below after testing 
 /* vijaya

  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rpt_gas_crude_products_pre_step1') 
		         
         drop table rpt_gas_crude_products_pre_step1

		 /* SELECT  MIN( EDS_FLOW_DATE_RANGE),  MAX( EDS_FLOW_DATE_RANGE) 
		     FROM rpt_prod_position_solo_hist_bak_4_21_2021
			 --rpt_prod_position_solo_hist
			 WHERE EDS_FLOW_DATE_RANGE IS NOT NULL*/

		/*  select  @lvd_min_date_hist= MIN( EDS_FLOW_DATE_RANGE), @lvd_max_date_hist  = MAX( EDS_FLOW_DATE_RANGE)    
		     FROM rpt_prod_position_solo_hist
			 WHERE EDS_FLOW_DATE_RANGE IS NOT NULL */

			 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rpt_gas_crude_products_pre_step0') 
		         
         drop table rpt_gas_crude_products_pre_step0

		   BEGIN TRY

		      select exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm,
					   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,                int_trdr_long_nm,     int_trdr_short_nm,
					   proc_eod_dt, tr_leg_fncl_phys_ind,   deal_undisc_delta_qty,
					/*   ( case when src_sys_nm = 'PLACEHOLDER' then 0 
							  when volume_uom_cd = 'BBL' then DEAL_UNDISC_DELTA_QTY
							  when VOLUME_UOM_CD = 'GAL' then DEAL_UNDISC_DELTA_QTY / 42 
							  when VOLUME_UOM_CD = 'CUBIC METER' then DEAL_UNDISC_DELTA_QTY / 6.28981 
							  when VOLUME_UOM_CD = 'MT' then DEAL_UNDISC_DELTA_QTY / 7.33 
							  when VOLUME_UOM_CD = 'MMBTU' then DEAL_UNDISC_DELTA_QTY end  )  UOM_ADJUSTED_POSITION ,
						( case when VOLUME_UOM_CD ='BBL' then 'BBL' 
							   when VOLUME_UOM_CD ='GAL' then 'BBL' 
							   when VOLUME_UOM_CD= 'CUBIC METER' then 'BBL' 
							   when VOLUME_UOM_CD= 'MT' then 'BBL' 
							   when VOLUME_UOM_CD= 'MMBTU' then 'MMBTU'
							   when VOLUME_UOM_CD ='GJ' then 'MMBTU'
							   else 'BUILD RULE' end ) VOLUME_UOM, */
						  0  RANK_EOD		
				---		DENSE_RANK() OVER  (PARTITION BY PROC_EOD_DT,SRC_SYS_NM,EXP_INDEX_NM,EXP_MO  ORDER BY PROC_EOD_DT,SRC_SYS_NM,EXP_INDEX_NM,EXP_MO ) ODW_AND_EDS_RANK
				into rpt_gas_crude_products_pre_step0
				  from  ddf_non_power_positions
				where  proc_eod_dt = '2021-02-26' and   src_sys_nm = 'EDS'
				  --   proc_eod_dt between @lvd_min_date_hist and @lvd_max_date_hist
				--  and  EXP_INDEX_NM LIKE 'PLT_%' ESCAPE '\'  

				/*	 from  ddf_non_power_positions
		 where  src_sys_nm = 'EDS'
		  AND proc_eod_dt > '2021-02-26'
		   and  EXP_INDEX_NM LIKE 'PLT_%' ESCAPE '\'*/


         	END TRY

					BEGIN CATCH
					   EXEC dbo.spErrorHandling 
					END CATCH



		   ---------

		   	 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rpt_gas_crude_products_pre_step1') 
		         
         drop table rpt_gas_crude_products_pre_step1

		   BEGIN TRY
     

				SELECT exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm,
					   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,                int_trdr_long_nm,     int_trdr_short_nm, 
					   proc_eod_dt, tr_leg_fncl_phys_ind,   deal_undisc_delta_qty, RANK_EOD, UOM_ADJUSTED_POSITION, VOLUME_UOM,  CAST( UOM_ADJUSTED_POSITION as float ) POSITION_FOR_NEXT_STEP,
					   ODW_AND_EDS_RANK, ODW_AND_EDS_RANK + 1 ODW_AND_EDS_RANK_PLUS1,
					   ( RANK_EOD +1 ) NEXT_DAY_RANK
					into rpt_gas_crude_products_pre_step1
				  FROM (
				select exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm,
					   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,                int_trdr_long_nm,     int_trdr_short_nm,
					   proc_eod_dt, tr_leg_fncl_phys_ind,   deal_undisc_delta_qty,
					   ( case when src_sys_nm = 'PLACEHOLDER' then 0 
							  when volume_uom_cd = 'BBL' then DEAL_UNDISC_DELTA_QTY
							  when VOLUME_UOM_CD = 'GAL' then DEAL_UNDISC_DELTA_QTY / 42 
							  when VOLUME_UOM_CD = 'CUBIC METER' then DEAL_UNDISC_DELTA_QTY / 6.28981 
							  when VOLUME_UOM_CD = 'MT' then DEAL_UNDISC_DELTA_QTY / 7.33 
							  when VOLUME_UOM_CD = 'MMBTU' then DEAL_UNDISC_DELTA_QTY end  )  UOM_ADJUSTED_POSITION ,
						( case when VOLUME_UOM_CD ='BBL' then 'BBL' 
							   when VOLUME_UOM_CD ='GAL' then 'BBL' 
							   when VOLUME_UOM_CD= 'CUBIC METER' then 'BBL' 
							   when VOLUME_UOM_CD= 'MT' then 'BBL' 
							   when VOLUME_UOM_CD= 'MMBTU' then 'MMBTU'
							   when VOLUME_UOM_CD ='GJ' then 'MMBTU'
							   else 'BUILD RULE' end ) VOLUME_UOM,
						  RANK_EOD, 		
						DENSE_RANK() OVER  (PARTITION BY PROC_EOD_DT,SRC_SYS_NM,EXP_INDEX_NM,EXP_MO  ORDER BY PROC_EOD_DT,SRC_SYS_NM,EXP_INDEX_NM,EXP_MO ) ODW_AND_EDS_RANK
						--DENSE_RANK() OVER  (PARTITION BY PROC_EOD_DT,EXP_INDEX_NM,EXP_MO  ORDER BY PROC_EOD_DT,EXP_INDEX_NM,EXP_MO ) ODW_AND_EDS_RANK
				 from  rpt_gas_crude_products_pre_step0 ---ddf_non_power_positions
				where  proc_eod_dt > '2021-02-25' and   src_sys_nm = 'EDS'
				  --   proc_eod_dt between @lvd_min_date_hist and @lvd_max_date_hist
				   and  EXP_INDEX_NM LIKE 'PLT_%' ESCAPE '\'  )  A 
				 -- where ODW_AND_EDS_RANK > 1

					END TRY

					BEGIN CATCH
					   EXEC dbo.spErrorHandling 
					END CATCH

					
   vijaya */
					---------------------------------

			IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rpt_gas_crude_products_step1') 
		         
         drop table rpt_gas_crude_products_step1

		 -- select count(1) from rpt_gas_crude_products_step1

			
		     BEGIN TRY

     
	 			select 
		               exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   
					   int_lentity_short_nm,			   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,               
					   int_trdr_long_nm,     int_trdr_short_nm,
					   proc_eod_dt, tr_leg_fncl_phys_ind,  deal_undisc_delta_qty,	cast( POSITION_FOR_NEXT_STEP  as float) POSITION_FOR_NEXT_STEP, POSITION_FOR_NEXT_STEP as UOM_ADJUSTED_POSITION,
					   VOLUME_UOM, RANK_EOD, ODW_AND_EDS_RANK, ODW_AND_EDS_RANK_PLUS1, NEXT_DAY_RANK
				into rpt_gas_crude_products_step1
				from 
				(			select exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm,
								   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,               
								   int_trdr_long_nm,     int_trdr_short_nm,
								   proc_eod_dt, tr_leg_fncl_phys_ind,  sum ( deal_undisc_delta_qty) deal_undisc_delta_qty,	sum(UOM_ADJUSTED_POSITION) POSITION_FOR_NEXT_STEP,
								   VOLUME_UOM, RANK_EOD, ODW_AND_EDS_RANK, ODW_AND_EDS_RANK_PLUS1, NEXT_DAY_RANK
						from  (
						 select * from  rpt_gas_crude_products_pre_step1 ) a
						group by exp_mo,      gridpt_nm,              inst_type_short_nm,   src_sys_nm,   volume_uom_cd,   hdr_fncl_phys_ind,   contrct_setl_dt,   tr_leg_delv_start_dt,   tr_leg_delv_end_dt,   ext_legal_entity_short_nm,   int_lentity_short_nm,
								   cdty_nm,     int_bnes_unit_short_nm, exp_index_nm,         bs_ind,       deal_track_no,   index_nm,            tr_term_begin_dt,  tr_term_end_dt,         tr_exec_dt,           portfolio_nm,                int_trdr_long_nm,     int_trdr_short_nm,
								   proc_eod_dt, tr_leg_fncl_phys_ind,
								   VOLUME_UOM, RANK_EOD, ODW_AND_EDS_RANK, ODW_AND_EDS_RANK_PLUS1, NEXT_DAY_RANK 
				) b

					END TRY

					BEGIN CATCH
					   EXEC dbo.spErrorHandling 
					END CATCH

			 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_1' )

     drop table intermediate_gas_crdue_step2_1

 -- select count(1) from intermediate_gas_crdue_step2_1  -- 2187-- 2188(spotfire)
 -- select count(1) from rpt_gas_crude_products_step1

 -- select count(1) from intermediate_gas_crdue_step2_1

  

-- step 2.1

      IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_1' )

     drop table intermediate_gas_crdue_step2_1

 -- select count(1) from intermediate_gas_crdue_step2_1

   BEGIN TRY

   select  bs_ind, cdty_nm, proc_eod_dt day_1,  deal_track_no, exp_index_nm exp_index_nm_system, ext_legal_entity_short_nm,  exp_mo,    hdr_fncl_phys_ind,  
          int_trdr_short_nm,  index_nm, int_bnes_unit_short_nm, int_lentity_short_nm, inst_type_short_nm, 
          odw_and_eds_rank_plus1 odwandedsrank,  portfolio_nm,  src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, 
          volume_uom, -- position_for_next_step position_day_1, 	
          sum( position_for_next_step ) as position_day_1
	into  intermediate_gas_crdue_step2_1
	from rpt_gas_crude_products_step1 s1 
  group by 
          bs_ind, cdty_nm, proc_eod_dt ,  
          deal_track_no,  exp_index_nm,   
          ext_legal_entity_short_nm,  exp_mo,    hdr_fncl_phys_ind,  
          int_trdr_short_nm,     index_nm, int_bnes_unit_short_nm, int_lentity_short_nm, inst_type_short_nm, 
          odw_and_eds_rank_plus1 , 
          portfolio_nm,  src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, 
          volume_uom

		  END TRY

		BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		END CATCH

		-- step 2.2

  IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_2' )

     drop table intermediate_gas_crdue_step2_2

	 -- select count(1) from intermediate_gas_crdue_step2_2

	  BEGIN TRY

     select  bs_ind, cdty_nm, proc_eod_dt day_2,  deal_track_no, exp_index_nm exp_index_nm_system, ext_legal_entity_short_nm,  exp_mo,    hdr_fncl_phys_ind,  
          int_trdr_short_nm,  index_nm, int_bnes_unit_short_nm, int_lentity_short_nm, inst_type_short_nm, 
          odw_and_eds_rank odwandedsrank,  portfolio_nm,  src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, 
          volume_uom, -- position_for_next_step position_day_1, 	
          sum( position_for_next_step ) as position_day_2
	  into intermediate_gas_crdue_step2_2
	  from rpt_gas_crude_products_step1 s1 
  group by 
          bs_ind, cdty_nm, proc_eod_dt ,  
          deal_track_no,  exp_index_nm,   
          ext_legal_entity_short_nm,  exp_mo,    hdr_fncl_phys_ind,  
          int_trdr_short_nm,     index_nm, int_bnes_unit_short_nm, int_lentity_short_nm, inst_type_short_nm, 
          odw_and_eds_rank , 
          portfolio_nm,  src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, 
          volume_uom 

		  END TRY

			BEGIN CATCH
			   EXEC dbo.spErrorHandling 
			END CATCH

-- --- step2.3

 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_3' )

       drop table intermediate_gas_crdue_step2_3

	   -- select count(1) from intermediate_gas_crdue_step2_3  -- 2187

	   BEGIN TRY

	   select step2_2.bs_ind,  step2_2.deal_track_no,  step2_2.exp_index_nm_system,  step2_2.exp_mo,  step2_2.hdr_fncl_phys_ind,  
         step2_2.index_nm,  step2_2.int_lentity_short_nm,  step2_2.inst_type_short_nm,  step2_2.odwandedsrank,  step2_2.src_sys_nm, 
		 step2_2.tr_leg_fncl_phys_ind, step2_2.volume_uom as unit,  step2_2.day_2,  step2_2.position_day_2,  step2_2.cdty_nm, 
		 step2_2.ext_legal_entity_short_nm, null as gridpt_nm, step2_2.int_trdr_short_nm, step2_2.int_bnes_unit_short_nm, step2_2.portfolio_nm, 
		 step2_2.tr_exec_dt,  step2_1.day_1,  step2_1.position_day_1
	   into 	     intermediate_gas_crdue_step2_3
       from  intermediate_gas_crdue_step2_2 step2_2 
         left outer join intermediate_gas_crdue_step2_1 step2_1
		   on step2_2.bs_ind = step2_1.bs_ind
          and step2_2.deal_track_no = step2_1.deal_track_no
          and step2_2.exp_index_nm_system = step2_1.exp_index_nm_system
          and step2_2.exp_mo =step2_1.exp_mo
          and step2_2.hdr_fncl_phys_ind = step2_1.hdr_fncl_phys_ind
          and step2_2.index_nm = step2_1.index_nm
          and step2_2.int_lentity_short_nm = step2_1.int_lentity_short_nm
          and step2_2.inst_type_short_nm = step2_1.inst_type_short_nm
          and step2_2.odwandedsrank = step2_1.odwandedsrank
          and step2_2.src_sys_nm = step2_1.src_sys_nm
          and step2_2.tr_leg_fncl_phys_ind =step2_1. tr_leg_fncl_phys_ind
          and step2_2.volume_uom = step2_1.volume_uom

		  -- step 2.4  -- no out put from 2.4 just an fyi
    --- below is reading from the network table

	--- select * from NETWORK_PRE_STG_PRODUCTS_MAPPING_SHEET1SHEET   -- 925
	    
	--	select  src_sys_nm,  exp_index_nm_system,  exp_index_nm_analysis, [group], [commodity], [% exposure]
	--	   from  NETWORK_PRE_STG_PRODUCTS_MAPPING_SHEET1SHEET

			END TRY

		BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		END CATCH

	   -- step 2.5

	   IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_5' )

	   drop table intermediate_gas_crdue_step2_5

	   -- select * from intermediate_gas_crdue_step2_5  -- 2187

	    BEGIN TRY
	  
	  select step2_3.*, n1.exp_index_nm_analysis, n1.[group], n1.[commodity], n1.[% exposure]    
	   into intermediate_gas_crdue_step2_5
	    from   intermediate_gas_crdue_step2_3 step2_3
		left outer join ( select n1.[group], n1.[commodity], n1.[% exposure], n1.exp_index_nm_system, n1.src_sys_nm , EXP_INDEX_NM_ANALYSIS
                            from network_pre_stg_products_mapping_sheet1sheet n1
                            group by src_sys_nm, exp_index_nm_system, EXP_INDEX_NM_ANALYSIS, [GROUP],   COMMODITY, [% EXPOSURE] ) n1
		--network_pre_stg_products_mapping_sheet1sheet n1
		   on     step2_3.exp_index_nm_system = n1.exp_index_nm_system
		     and  step2_3.src_sys_nm = n1.src_sys_nm 

	 END TRY

		BEGIN CATCH
		   EXEC dbo.spErrorHandling 
		END CATCH

 
       
	   -- step 2.6

	   -- select * from intermediate_gas_crdue_step2_5

	   -- select count(1) from intermediate_gas_crdue_step2_final  -- 2187 - same as Spotfire.

	   IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_step2_final' )

	   drop table intermediate_gas_crdue_step2_final

	   BEGIN TRY

	  --  select max(day_1) from intermediate_gas_crdue_step2_5 where day_1 is not null

	  
	       set @lvd_max_date = ( select max(day_1) from intermediate_gas_crdue_step2_5 )
		   set @lvd_max_date_for_source =   ( select max(day_1) 
		                                          from intermediate_gas_crdue_step2_5 
		                                          group by src_sys_nm )

       	select  
					bs_ind, cdty_nm, day_1, deal_track_no, exp_index_nm_system, ext_legal_entity_short_nm, exp_mo, gridpt_nm, hdr_fncl_phys_ind, int_trdr_short_nm, index_nm, int_bnes_unit_short_nm, int_lentity_short_nm,
					inst_type_short_nm, odwandedsrank, portfolio_nm, src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, unit, position_day_1, day_2,position_day_2, exp_index_nm_analysis, [group], commodity, [% exposure],
					daily_total_position, 
					       ( case when COMMODITY ='EXCLUDE - NOT REPORTED' then 'EXCLUDE - UNREPORTED POSITIION' 
								  when @lvd_max_date_for_source = day_1 then   'EXCLUDE - NEED ONE MORE DAY FOR CALCS'   --day_1 is null then
								  when EXT_LEGAL_ENTITY_SHORT_NM  in ('ACCTG_ADJ_EXT_LGL', 'ZZZ_ACRUAL_LE') then 'EXCLUDE - BASED ON EXT_LEGAL_ENTITY'
								  when Round(daily_total_position,2)=0.0 then 'EXCLUDE - NO VOLUME FLOW'
								  when Round(Sum(daily_total_position) over (partition by src_sys_nm, exp_mo,exp_index_nm_system),2)=0.0 then 'EXCLUDE - NET POSITION 0' -- in the outer query
								  when position_day_2 = position_day_1 then 'EXCLUDE - NO FLOW'
								  when Min(exp_mo) over ( partition by [1st_flow],src_sys_nm,exp_index_nm_analysis)<> exp_mo then 'EXCLUDE - FORWARD MONTH'
								  when (position_day_1>0) and (position_day_2>position_day_1) then 'EXCLUDE - APPEARS VOLUME WAS ADJUSTED'
								  when (position_day_1<0) and (position_day_2<position_day_1) then 'EXCLUDE - APPEARS VOLUME WAS ADJUSTED'
								  when (position_day_2) is null then 'KEEP'
								else 'KEEP'	end )  as [rule],
					[1st_flow],  upper(  datename( weekday , ( case when day_2 is not null then day_2 else first_flow_step2 end ) ) ) day_of_week,
				  first_flow_step2
		INTO intermediate_gas_crdue_step2_final
 from (
			select  
					bs_ind, cdty_nm, day_1, deal_track_no, exp_index_nm_system, ext_legal_entity_short_nm, exp_mo, gridpt_nm, hdr_fncl_phys_ind, int_trdr_short_nm, index_nm, int_bnes_unit_short_nm, int_lentity_short_nm,
					inst_type_short_nm, odwandedsrank, portfolio_nm, src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, unit, position_day_1, day_2,position_day_2, exp_index_nm_analysis, [group], commodity, [% exposure],
					daily_total_position,
					first_flow_step2, 
									   --- 1st_flow
					   ( case when day_2 is not null then day_2 else first_flow_step2 end )  as [1st_flow]
							 --  upper(  datename( weekday , ( case when day_2 is not null then day_2 else first_flow_step2 end ) ) ) day_of_week
			             from  
								( select 
							          bs_ind, cdty_nm, day_1, deal_track_no, exp_index_nm_system, ext_legal_entity_short_nm, exp_mo, gridpt_nm, hdr_fncl_phys_ind, int_trdr_short_nm, index_nm, int_bnes_unit_short_nm, int_lentity_short_nm,
							          inst_type_short_nm, odwandedsrank, portfolio_nm, src_sys_nm, tr_leg_fncl_phys_ind, tr_exec_dt, unit, position_day_1, day_2,position_day_2, exp_index_nm_analysis, [group], commodity, [% exposure], 
									( case when step2_5.src_sys_nm  <>'SYNAPSE' and @lvd_max_date = day_1 then 0 
															--day_1 is null then 0
															when step2_5.src_sys_nm = 'SYNAPSE' then (position_day_1)
															when step2_5.src_sys_nm in ('ODW', 'EDS') and position_day_1 is null then 0 
															when step2_5.src_sys_nm in ('ODW', 'EDS') and position_day_2 is null then position_day_1 
															when step2_5.src_sys_nm in ('ODW', 'EDS') and position_day_1 is not null and position_day_2 is not null then (position_day_1 -  position_day_2 ) else null end  ) daily_total_position,
							-- get the rule column in the outer query   
							-- get the 1st_flow in the outer query  -- check below with Mrunali
															min(day_2) over( partition by src_sys_nm, day_1) as first_flow_step2		     
							-- get the day_of_week in the outer query
										from 	intermediate_gas_crdue_step2_5 step2_5 ) a  ) b    

      END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH


	  -- step pre_export

	  --- select count(1) from intermediate_gas_crdue_pre_export  -- 354 - SPotfire 355

	  -- select * from intermediate_gas_crdue_pre_export

	  -- select * from    INFORMATION_SCHEMA.columns WHERE TABLE_NAME = 'rpt_prod_position_solo_hist'

 IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'intermediate_gas_crdue_pre_export' )

	   drop table intermediate_gas_crdue_pre_export

	   -- select * from information_schema.columns where table_name = 'intermediate_gas_crdue_pre_export'
	   -- select * from intermediate_gas_crdue_pre_export

	   delete from rpt_prod_position_solo_hist where first_flow is null

	     BEGIN TRY
 

	   select day_of_week, [1st_flow], initial_rule, [% exposure], commodity, [group], exp_index_nm_analysis,  exp_mo, inst_type_short_nm, src_sys_nm, hdr_fncl_phys_ind, ext_legal_entity_short_nm, int_lentity_short_nm_pre_adj, 
       int_bnes_unit_short_nm, exp_index_nm_system, bs_ind, portfolio_nm, int_trdr_short_nm, unit, daily_total_position_pre_adj, max_reported_1st_flow,  int_lentity_short_nm, total_position, 
	        ( case when (max_reported_1st_flow>=[1st_flow]) and (initial_rule ='KEEP') then 'EXCLUDE - ALREADY REPORTED'
				when initial_rule like '%EXCLUDE%' then initial_rule  
				when ext_legal_entity_short_nm in ('ZZZ_ACCRUAL_LE', 'ZZZ_DUMMY_LE') then 'EXCLUDE - BASED ON COUNTERPARTY'
				when Abs(Sum(total_position) over (partition by [1st_flow], src_sys_nm, int_trdr_short_nm, exp_index_nm_system, exp_mo,initial_rule, ext_legal_entity_short_nm,int_lentity_short_nm, portfolio_nm, 
				        inst_type_short_nm, hdr_fncl_phys_ind))<1 then 'EXCLUDE - EXTERNAL ENTITY AGGREGATED AT PORTFOLIIO AND INSTRUMENT TYPE LEVEL Abs(NET POSITION) < 1 UNIT' 
				when Abs(Sum(total_position) over ( partition by [1st_flow], src_sys_nm, int_trdr_short_nm, exp_index_nm_system, exp_mo,initial_rule, ext_legal_entity_short_nm,int_lentity_short_nm, 
				        portfolio_nm, hdr_fncl_phys_ind))<1 then 'EXCLUDE - EXTERNAL ENTITY AGGREGATED AT PORTFOLIIO LEVEL Abs(NET POSITION) < 1 UNIT' 
				when Abs(Sum(total_position) over (partition by [1st_flow], src_sys_nm, int_trdr_short_nm, exp_index_nm_system, exp_mo, initial_rule, int_lentity_short_nm,hdr_fncl_phys_ind))<1 
				    then 'EXCLUDE - EXTERNAL ENTITY AGGREGATED Abs(NET POSITION) < 1 UNIT' 
				when Abs(Sum(total_position) over (partition by [1st_flow], src_sys_nm, int_trdr_short_nm, exp_index_nm_system, exp_mo,initial_rule, int_lentity_short_nm,[EXT_LEGAL_ENTITY_SHORT_NM],hdr_fncl_phys_ind))<1 
				then 'EXCLUDE - AGGREGATED Abs(NET POSITION) < 1 UNIT'
				when total_position is null then 'EXCLUDE - NULL POSITION'
				else 'KEEP' end ) as [rule]
      into      intermediate_gas_crdue_pre_export
	from    
	   ( 	select daily_total_position_pre_adj,  day_of_week, initial_rule, exp_index_nm_analysis,ext_legal_entity_short_nm, portfolio_nm,
			--	[1st_flow],  step 2.1
			  ( case when step_pre1.src_sys_nm in ('SYNAPSE') and [group] in ('MONTHLY' ) then ( substring(cast( concat(exp_mo,'1') as char(8))  ,1,4)+'/'+ substring(  cast( concat(exp_mo,'1') as char(8))   ,5,2)+'/'+ '1')  else  [1st_flow] end ) as [1st_flow],
				 [% exposure], commodity, [group],  exp_mo, 
				inst_type_short_nm, step_pre1.src_sys_nm, hdr_fncl_phys_ind, 
				-- step 4.1
					 int_lentity_short_nm_pre_adj,
			 ( case when int_lentity_short_nm_pre_adj in ('SENA', 'SENA_LGL', 'CORAL TP&S_LGL') then 'SENA'
					when int_lentity_short_nm_pre_adj in ('SENAC', 'SENAC_LGL') then 'SENAC'
					when int_lentity_short_nm_pre_adj in ('STCAN', 'STCAN - LE') then 'STCAN'
					when int_lentity_short_nm_pre_adj in ('STRM', 'STRM_LGL') then 'STRM'
					when int_lentity_short_nm_pre_adj in ('STUSCO - LE', 'STUSCO-LE') then 'STUSCO' 
					when int_lentity_short_nm_pre_adj in ('SHELL CANADA PRODUCT-RSK') then 'SCP' 
					when int_lentity_short_nm_pre_adj in ('SHELL CANADA PRODUCTS LTD-LE') then 'SCP'
					when int_lentity_short_nm_pre_adj in ('SHLTRMX_LGL', 'SHLTRMX') then 'SHLTRMX' 
					when int_lentity_short_nm_pre_adj in ('BGEMCAN_LGL') then 'BGEMCAN' 
					when int_lentity_short_nm_pre_adj in ('SHELL NA L_LGL') then 'SNLNG'
					else 'BUILD RULE'
					end ) as int_lentity_short_nm,
				-- step 4.2
				(daily_total_position_pre_adj*[% exposure]) as total_position,
				-- step 4.3  -- Has to be in outer layer as it needs the above 1st_flow to be calculated first.
			--	( case when  hist.max_reported_1st_flow >= 
		        int_bnes_unit_short_nm, exp_index_nm_system, bs_ind,  int_trdr_short_nm, unit,	 hist.max_reported_1st_flow
		   from (
					  select sum (daily_total_position) as  daily_total_position_pre_adj,  day_of_week, [1st_flow], [rule] as initial_rule, [% exposure], commodity, [group], exp_index_nm_analysis, exp_mo, 
							 inst_type_short_nm, f.src_sys_nm, hdr_fncl_phys_ind, int_lentity_short_nm as int_lentity_short_nm_pre_adj,  int_bnes_unit_short_nm, exp_index_nm_system, bs_ind, portfolio_nm, upper(int_trdr_short_nm) as int_trdr_short_nm, unit,
							 ext_legal_entity_short_nm
							-- hist.max_reported_1st_flow
						from intermediate_gas_crdue_step2_final f
					   group by 
						 day_of_week, [1st_flow], [rule] , [% exposure], commodity, [group], exp_index_nm_analysis, exp_mo, 
						 inst_type_short_nm, src_sys_nm, hdr_fncl_phys_ind, int_lentity_short_nm ,  int_bnes_unit_short_nm, exp_index_nm_system, bs_ind, portfolio_nm, upper(int_trdr_short_nm) , unit, ext_legal_entity_short_nm
							 ) step_pre1, 
					 ( select max(first_flow) as max_reported_1st_flow, src_sys_nm
						 from rpt_prod_position_solo_hist
						group by src_sys_nm ) hist -- step2.1
	    where step_pre1.src_sys_nm = hist.src_sys_nm  ) final_step

		END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH

		--- history

		--  select count(1) from product_scorecard_position_solo_export
		-- select count(1) from rpt_prod_position_solo_hist  -- 229,496

	   -- select * from information_schema.columns where table_name = 'product_scorecard_position_solo_export'
	   -- select * from rpt_prod_position_solo_hist

	   -- select * from intermediate_gas_crdue_pre_export

	     BEGIN TRY
 

	  merge rpt_prod_position_solo_hist as t 
	 using (
	  select  e.day_of_week, e.[1st_flow] first_flow , e.commodity,  e.[group] group1, e.exp_index_nm_analysis,  e.exp_mo,   e.inst_type_short_nm,  e.src_sys_nm,  e.hdr_fncl_phys_ind,  e.ext_legal_entity_short_nm,
			  e.int_bnes_unit_short_nm,  e.portfolio_nm,  e.int_trdr_short_nm,  e.unit,  e.int_lentity_short_nm,  e.total_position,
			  --  when Min(exp_mo) over ( partition by [1st_flow],src_sys_nm,exp_index_nm_analysis)<> exp_mo then 'EXCLUDE - FORWARD MONTH'
		--	  ( case when e.[1st_flow] = (  max(e.[1st_flow] ) over ( partition by e.src_sys_nm)  ) then e.[1st_flow] 
		---	         when e.[1st_flow] <> (  max(e.[1st_flow] ) over ( partition by e.src_sys_nm)  ) then getdate() else null end ) eds_flow_date_range
		   ( case when charindex ( 'EDS', e.SRC_SYS_NM) <>0 AND e.SRC_SYS_NM <> 'EDS' THEN cast (getdate() as date)  
			          when charindex ( 'EDS', e.SRC_SYS_NM) = 0 AND rank() over ( partition by e.src_sys_nm order by convert( varchar, e.src_sys_nm ) ) = 1 then cast (getdate() as date) 
					  when charindex ( 'EDS', e.SRC_SYS_NM) = 0 AND rank() over ( partition by e.src_sys_nm  order by convert( varchar, e.src_sys_nm ) ) <> 1 then cast ('2018-01-01' as date) 
					  when max(e.[1st_flow]) over ( partition by e.src_sys_nm  ) = e.[1st_flow] and e.src_sys_nm = 'EDS' then e.[1st_flow]
					  when max(e.[1st_flow]) over ( partition by e.src_sys_nm  ) <> e.[1st_flow] and e.src_sys_nm = 'EDS' then cast (getdate() as date) 
					  else null end ) eds_flow_date_range	
	  from    intermediate_gas_crdue_pre_export e 
	  where  e.[rule] = 'KEEP' ) stg
		 on (     stg.day_of_week = t.day_of_week and  stg.first_flow = t.first_flow and  stg.commodity = t.commodity and  stg.group1 = t.group1 and stg.exp_index_nm_analysis = t.exp_index_nm_analysis   
		    and   stg.exp_mo = t.exp_mo and   stg.inst_type_short_nm = t.inst_type_short_nm and   stg.src_sys_nm = t.src_sys_nm and  stg.hdr_fncl_phys_ind = t.hdr_fncl_phys_ind 
			and   stg.ext_legal_entity_short_nm = t.ext_legal_entity_short_nm and   stg.int_bnes_unit_short_nm = t.int_bnes_unit_short_nm and stg.portfolio_nm = t.portfolio_nm 
			and   stg.int_trdr_short_nm = t.int_trdr_short_nm and  stg.unit = t.unit and stg.int_lentity_short_nm = t.int_lentity_short_nm and  stg.total_position = t.total_position )
			--and   stg.eds_flow_date_range = t.eds_flow_date_range )
		 when not matched then 
		  insert (      day_of_week, first_flow, commodity, group1, exp_index_nm_analysis
					  , exp_mo , inst_type_short_nm, src_sys_nm , hdr_fncl_phys_ind, ext_legal_entity_short_nm
					  , int_bnes_unit_short_nm, portfolio_nm, int_trdr_short_nm, unit, int_lentity_short_nm
					  , total_position, eds_flow_date_range, update_ts  )
			values (   stg.day_of_week, stg.first_flow, stg.commodity, stg.group1, stg.exp_index_nm_analysis
					  , stg.exp_mo , stg.inst_type_short_nm, stg.src_sys_nm , stg.hdr_fncl_phys_ind, stg.ext_legal_entity_short_nm
					  , stg.int_bnes_unit_short_nm, stg.portfolio_nm, stg.int_trdr_short_nm, stg.unit, stg.int_lentity_short_nm
					  , stg.total_position, stg.eds_flow_date_range, getdate() ) ;

					  delete from rpt_prod_position_solo_hist where first_flow is null

		 END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH



		IF EXISTS 
		  (SELECT 
             TABLE_NAME 
         FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'product_scorecard_position_solo_export' )

	   drop table product_scorecard_position_solo_export

	   -- select * from information_schema.columns where table_name = 'product_scorecard_position_solo_export'
	   -- select * from rpt_prod_position_solo_hist

	   -- select * from intermediate_gas_crdue_pre_export

	     BEGIN TRY 

	  select *  into product_scorecard_position_solo_export
	   from rpt_prod_position_solo_hist
	   
		 END TRY

	  BEGIN CATCH
      EXEC dbo.spErrorHandling 
      END CATCH

end

       

 --	
	
	
	
					

    
	  
	  


 ---  grant execute on [dbo].[usp_merge_report_layer_prc] to vbhattaru


 --- export the final step 

     --- select e.[1st_flow] from intermediate_gas_crdue_pre_export e
	 -- select rule from rpt_prod_position_solo_hist  -- 236369  -- 2018-01-03 00:00:00.000	2021-04-14 00:00:00.000
/*
	 
	 merge rpt_prod_position_solo_hist as t 
	 using (
	  select  e.day_of_week, e.[1st_flow] first_flow , e.commodity,  e.[group] group1, e.exp_index_nm_analysis,  e.exp_mo,   e.inst_type_short_nm,  e.src_sys_nm,  e.hdr_fncl_phys_ind,  e.ext_legal_entity_short_nm,
			  e.int_bnes_unit_short_nm,  e.portfolio_nm,  e.int_trdr_short_nm,  e.unit,  e.int_lentity_short_nm,  e.total_position,
			  --  when Min(exp_mo) over ( partition by [1st_flow],src_sys_nm,exp_index_nm_analysis)<> exp_mo then 'EXCLUDE - FORWARD MONTH'
		--	  ( case when e.[1st_flow] = (  max(e.[1st_flow] ) over ( partition by e.src_sys_nm)  ) then e.[1st_flow] 
		---	         when e.[1st_flow] <> (  max(e.[1st_flow] ) over ( partition by e.src_sys_nm)  ) then getdate() else null end ) eds_flow_date_range
		   ( case when charindex ( 'EDS', e.SRC_SYS_NM) <>0 AND e.SRC_SYS_NM <> 'EDS' THEN cast (getdate() as date)  
			          when charindex ( 'EDS', e.SRC_SYS_NM) = 0 AND rank() over ( partition by e.src_sys_nm order by convert( varchar, e.src_sys_nm ) ) = 1 then cast (getdate() as date) 
					  when charindex ( 'EDS', e.SRC_SYS_NM) = 0 AND rank() over ( partition by e.src_sys_nm  order by convert( varchar, e.src_sys_nm ) ) <> 1 then cast ('2018-01-01' as date) 
					  when max(e.[1st_flow]) over ( partition by e.src_sys_nm  ) = e.[1st_flow] and e.src_sys_nm = 'EDS' then e.[1st_flow]
					  when max(e.[1st_flow]) over ( partition by e.src_sys_nm  ) <> e.[1st_flow] and e.src_sys_nm = 'EDS' then cast (getdate() as date) 
					  else null end ) eds_flow_date_range	
	  from    intermediate_gas_crdue_pre_export e ) stg
	--	 and  e.[rule] = 'KEEP' ) stg
		 on (     stg.day_of_week = t.day_of_week and  stg.first_flow = t.first_flow and  stg.commodity = t.commodity and  stg.group1 = t.group1 and stg.exp_index_nm_analysis = t.exp_index_nm_analysis   
		    and   stg.exp_mo = t.exp_mo and   stg.inst_type_short_nm = t.inst_type_short_nm and   stg.src_sys_nm = t.src_sys_nm and  stg.hdr_fncl_phys_ind = t.hdr_fncl_phys_ind 
			and   stg.ext_legal_entity_short_nm = t.ext_legal_entity_short_nm and   stg.int_bnes_unit_short_nm = t.int_bnes_unit_short_nm and stg.portfolio_nm = t.portfolio_nm 
			and   stg.int_trdr_short_nm = t.int_trdr_short_nm and  stg.unit = t.unit and stg.int_lentity_short_nm = t.int_lentity_short_nm and  stg.total_position = t.total_position )
			--and   stg.eds_flow_date_range = t.eds_flow_date_range )
		 when not matched then 
		  insert (      day_of_week, first_flow, commodity, group1, exp_index_nm_analysis
					  , exp_mo , inst_type_short_nm, src_sys_nm , hdr_fncl_phys_ind, ext_legal_entity_short_nm
					  , int_bnes_unit_short_nm, portfolio_nm, int_trdr_short_nm, unit, int_lentity_short_nm
					  , total_position, eds_flow_date_range, update_ts  )
			values (   stg.day_of_week, stg.first_flow, stg.commodity, stg.group1, stg.exp_index_nm_analysis
					  , stg.exp_mo , stg.inst_type_short_nm, stg.src_sys_nm , stg.hdr_fncl_phys_ind, stg.ext_legal_entity_short_nm
					  , stg.int_bnes_unit_short_nm, stg.portfolio_nm, stg.int_trdr_short_nm, stg.unit, stg.int_lentity_short_nm
					  , stg.total_position, stg.eds_flow_date_range, getdate() ) ;
END  -- PROC

 */

GO



SET echo ON;
TRUNCATE TABLE ENDUR.QUERY_RESULT ;
EXEC DBMS_STATS.GATHER_TABLE_STATS('ENDUR','QUERY_RESULT', CASCADE=>TRUE);
TRUNCATE TABLE ENDUR.WFLOW_RUNNING;
EXEC DBMS_STATS.GATHER_TABLE_STATS('ENDUR','WFLOW_RUNNING', CASCADE=>TRUE);
TRUNCATE TABLE ENDUR.JOB_RUNNING;
EXEC DBMS_STATS.GATHER_TABLE_STATS('ENDUR','JOB_RUNNING', CASCADE=>TRUE);
TRUNCATE TABLE ENDUR.WFLOW_WAITING;
EXEC DBMS_STATS.GATHER_TABLE_STATS('ENDUR','WFLOW_WAITING', CASCADE=>TRUE);
UPDATE endur.user_sqlldr_config SET path = REPLACE (path, 'D:', 'E:\APPS');
COMMIT;
DROP PUBLIC DATABASE LINK GPNA2;
DROP PUBLIC DATABASE LINK INTLINK;
DROP PUBLIC DATABASE LINK MCC_LINK;
DROP PUBLIC DATABASE LINK R14PROD;
DROP PUBLIC DATABASE LINK ZEMLINK;
DROP PUBLIC DATABASE LINK ZE_WORLD_PROD;
DROP PUBLIC DATABASE LINK DBLINK;
CREATE PUBLIC DATABASE LINK DBLINK
  CONNECT TO DBLINK IDENTIFIED BY dblink USING 'TESLD08T';
DROP PUBLIC DATABASE LINK ECLINK;
CREATE PUBLIC DATABASE LINK ECLINK 
CONNECT TO ECKERNEL_ECBGSGM IDENTIFIED BY energy 
USING  'TESLD33A';
DROP PUBLIC DATABASE LINK SHLZEMLINK;
CREATE PUBLIC DATABASE LINK SHLZEMLINK CONNECT TO "SHL_SLMTENDURREMOTEUSER" IDENTIFIED BY "SUN$hine123#" USING '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP) (HOST= zema-prd.shell.com)(PORT=1522)) (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME= ZEPP01)))';
alter user endur identified by endur;
alter user dblink identified by dblink;
ALTER session SET current_schema = ENDUR;
  DELETE FROM bg_rep_authentication;
  INSERT
  INTO bg_rep_authentication VALUES
    (
      'TESLD08T',
      'DW',
      '9zAyyjhppimz',
      '9zAyyjhppimz'
    );
  INSERT
  INTO BG_REP_AUTHENTICATION
    (
      SERVER_NAME,
      SCHEMA,
      LOGIN,
      PASSWORD
    )
    VALUES
    (
      'TESLD08T',
      'EDW',
      '9zAyyjhppimz',
      '9zAyyjhppimz'
    );
  INSERT
  INTO ENDUR.BG_REP_AUTHENTICATION VALUES
    (
      'SSGMHO2',
      'ECKERNEL_ECBGSGM',
      'AFHEIFDTKIGFyHAOZzV',
      'aqspcsqiv'
    );
  DELETE FROM user_rep_report_deliveries;
  UPDATE global_env_settings
  SET env_value    = 'TESLD07T',
    user_id        = 1,
    last_update    = SYSDATE
  WHERE env_id     = 20160
  AND personnel_id = -1;
update endur.service_mgr set workstation_name='AMSDC1-S-91557' where APP_LOGIN_NAME='appserver1';
update endur.service_mgr set workstation_name='AMSDC1-S-91558' where APP_LOGIN_NAME='appserver2';
update endur.service_mgr set workstation_name='AMSDC1-S-91559' where APP_LOGIN_NAME='appserver3';
update endur.service_mgr set workstation_name='AMSDC1-S-91523' where APP_LOGIN_NAME='appserver21';
update endur.service_mgr set workstation_name='AMSDC1-S-91524' where APP_LOGIN_NAME='appserver22';
update endur.service_mgr set workstation_name='AMSDC1-S-91525' where APP_LOGIN_NAME='appserver23';
update endur.service_mgr set workstation_name='AMSDC1-S-91526' where APP_LOGIN_NAME='appserver24';
update endur.service_mgr set workstation_name='AMSDC1-S-91527' where APP_LOGIN_NAME='appserver25';
update endur.service_mgr set workstation_name='AMSDC1-S-91528' where APP_LOGIN_NAME='appserver26';
update endur.service_mgr set workstation_name='AMSDC1-S-91529' where APP_LOGIN_NAME='appserver27';
update endur.service_mgr set workstation_name='AMSDC1-S-91530' where APP_LOGIN_NAME='appserver28';
update endur.service_mgr set workstation_name='AMSDC1-S-91531' where APP_LOGIN_NAME='appserver29';
update endur.service_mgr set workstation_name='AMSDC1-S-91917'  where APP_LOGIN_NAME='appserver30';
update endur.service_mgr set workstation_name='AMSDC1-S-91918'  where APP_LOGIN_NAME='appserver31';
update endur.service_mgr set workstation_name='AMSDC1-S-91919'  where APP_LOGIN_NAME='appserver32';
update endur.service_mgr set workstation_name='AMSDC1-S-91920'  where APP_LOGIN_NAME='appserver33';
update endur.service_mgr set workstation_name='AMSDC1-S-91921'  where APP_LOGIN_NAME='appserver34';
update endur.service_mgr set workstation_name='AMSDC1-S-91922'  where APP_LOGIN_NAME='appserver35';
update endur.service_mgr set workstation_name='AMSDC1-S-91923'  where APP_LOGIN_NAME='appserver36';
update endur.service_mgr set workstation_name='AMSDC1-S-91924'  where APP_LOGIN_NAME='appserver37';
update endur.service_mgr set workstation_name='AMSDC1-S-91925'  where APP_LOGIN_NAME='appserver38';
update endur.service_mgr set workstation_name='AMSDC1-S-91926'  where APP_LOGIN_NAME='appserver39';
update endur.service_mgr set workstation_name='AMSDC1-S-91927'  where APP_LOGIN_NAME='appserver40';
update endur.service_mgr set workstation_name='AMSDC1-S-91928'  where APP_LOGIN_NAME='appserver41';
update endur.service_mgr set workstation_name='AMSDC1-S-91929'  where APP_LOGIN_NAME='appserver42';
update endur.service_mgr set workstation_name='AMSDC1-S-91930'  where APP_LOGIN_NAME='appserver43';
update endur.service_mgr set workstation_name='AMSDC1-S-91931'  where APP_LOGIN_NAME='appserver44';
update endur.service_mgr set workstation_name='AMSDC1-S-91932'  where APP_LOGIN_NAME='appserver45';
update endur.service_mgr set workstation_name='AMSDC1-S-91933'  where APP_LOGIN_NAME='appserver46';
update endur.service_mgr set workstation_name='AMSDC1-S-91955'  where APP_LOGIN_NAME='appserver47';
update endur.service_mgr set workstation_name='AMSDC1-S-91956'  where APP_LOGIN_NAME='appserver48';
update endur.service_mgr set workstation_name='AMSDC1-S-91556'  where APP_LOGIN_NAME='appserver49';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG2-EU-S'  WHERE APP_LOGIN_NAME='appserver1';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG2-EU-S'  WHERE APP_LOGIN_NAME='appserver2';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG2-EU-S'  WHERE APP_LOGIN_NAME='appserver3';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver21';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver22';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver23';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver24';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver25';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver26';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver27';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver28';
update endur.service_mgr set LOGIN_NAME='Endur_LNG_UAT-EU-S'    WHERE APP_LOGIN_NAME='appserver29';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver30';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver31';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver32';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver33';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver34';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver35';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver36';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver37';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver38';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG3-EU-S'  WHERE APP_LOGIN_NAME='appserver39';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver40';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver41';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver42';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver43';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver44';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver45';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver46';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver47';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG4-EU-S'  WHERE APP_LOGIN_NAME='appserver48';
update endur.service_mgr set LOGIN_NAME='Endur-LNG-UATG2-EU-S'  WHERE APP_LOGIN_NAME='appserver49';
commit;
DELETE
  FROM pers_license_types_link;
 DELETE
  FROM pers_license_types_link
  WHERE personnel_id NOT IN
    (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id IN (8, 20004, 20005, 20006, 20007,20011)
    );
 UPDATE personnel
  SET status         =2,
    personnel_type   =1
  WHERE status       =1
  AND personnel_type = 2
  AND id_number NOT IN
    (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id IN (8,20004, 20005, 20006, 20007, 20011)
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id
  FROM personnel_functional_group
  WHERE func_group_id        = 20007
  ) pfg ON (pltl.personnel_id=pfg.personnel_id)
WHEN matched THEN
  UPDATE SET license_type=1 WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      1
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id FROM personnel_functional_group WHERE func_group_id = 8
  )
  pfg ON (pltl.personnel_id=pfg.personnel_id)
WHEN matched THEN
  UPDATE SET license_type=2 WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      2
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id = 20006
  )
  pfg ON (pltl.personnel_id=pfg.personnel_id)
WHEN matched THEN
  UPDATE SET license_type=19 WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      19
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id = 20004
  )
  pfg ON (pltl.personnel_id=pfg.personnel_id)
WHEN matched THEN
  UPDATE SET license_type=0 WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      0
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id = 20005
  )
  pfg ON (pltl.personnel_id=pfg.personnel_id)
WHEN matched THEN
  UPDATE SET license_type=19 WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      19
    );
  merge INTO pers_license_types_link pltl USING
  (SELECT personnel_id,
      23 license_type
    FROM personnel_functional_group
    WHERE func_group_id = 20011
  )
  pfg ON (pltl.personnel_id=pfg.personnel_id AND pltl.license_type = pfg.license_type)
WHEN NOT matched THEN
  INSERT
    (
      pltl.personnel_id,
      pltl.license_type
    )
    VALUES
    (
      pfg.personnel_id,
      23
    );
   merge INTO users_to_groups utg USING
  (SELECT DISTINCT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id IN (8, 20004, 20005, 20006, 20007,20011)
    AND personnel_id NOT IN(SELECT ba.personnel_id FROM personnel_info ba, personnel_info team WHERE ba.type_id = 20001 AND ba.info_value = 'Australia'
    AND team.type_id =20002 AND team.info_value <>'SME'
    AND ba.personnel_id = team.personnel_id)
  )
  pfg ON (utg.group_number=1 AND utg.user_number = pfg.personnel_id)
WHEN NOT matched THEN
  INSERT
    (utg.user_number, utg.group_number
    ) VALUES
    (pfg.personnel_id, 1
    ); 
  INSERT
  INTO users_to_groups
  SELECT a.user_number,
    20025
  FROM users_to_groups a
  WHERE a.Group_Number = 1
  AND NOT EXISTS
    (SELECT 1
    FROM users_to_groups b
    WHERE a.user_number = b.user_number
    AND b.group_number  = 20025
    );
  COMMIT;
  UPDATE personnel
  SET status              =1,
    Personnel_Type        = 2,
    password_never_expires=1
  WHERE personnel_type   <> 0
  AND id_number          IN
    (SELECT personnel_id
    FROM personnel_functional_group
    WHERE func_group_id IN (8, 20004, 20005, 20006, 20007,20011)
    );
  UPDATE endur.job_cfg
  SET schedule_0  =0
  WHERE type      =0
  AND service_type=1;
  UPDATE orien_push_config
  SET delivery_subject    = ' '
  WHERE delivery_subject <> ' ';
  UPDATE party_address
  SET fax    = ' '
  WHERE fax <> ' ';
  UPDATE grid_property_overlays
  SET prop_value = ' '
  WHERE prop_id IN
    (SELECT grid_default_properties.prop_id
    FROM grid_default_properties
    WHERE grid_default_properties.prop_label IN ('IceCmUserName', 'ICE.UserName', 'ICEOTC.UserName', 'ICEFUTS.UserName','Connection String','Database User Name')
    );
  UPDATE configuration
  SET prev_business_date =
    (SELECT MAX(run_time)
    FROM sim_header
    WHERE run_type = 1
    AND run_time   <
      (SELECT MAX(run_time) FROM sim_header WHERE run_type = 1
      )
    ) ,
    business_date =
    (SELECT MAX(run_time) FROM sim_header WHERE run_type = 1
    ) ,
    processing_date =
    (SELECT MAX(run_time) FROM sim_header WHERE run_type = 1
    ) ,
    trading_date =
    (SELECT MAX(run_time) FROM sim_header WHERE run_type = 1
    ) ,
    eod_date =
    (SELECT MAX(run_time) FROM sim_header WHERE run_type = 1
    )
  WHERE 1=1;
  DELETE
  FROM grid_property_overlays
  WHERE prop_id = 57;
  COMMIT;
  DELETE
  FROM endur.user_notify_email_address em1
  WHERE rowid IN
    (SELECT em1.rowid
    FROM endur.user_notify_email_address em1
    JOIN
      (SELECT rowid rowid1,
        rank() OVER (PARTITION BY email_group ORDER BY email_address) rn,
        em.*
      FROM endur.user_notify_email_address em
	  WHERE lower(type_code) not in ('frm') 
      ) em2
    ON em1.rowid = em2.rowid1
    WHERE em2.rn > 1
    );
  UPDATE endur.user_notify_email_address em
  SET email_address = 'Endur_EOD_Notifications@shell.com'
  WHERE lower(type_code) not in ('frm');
  UPDATE endur.user_notify_email_address em
  SET email_address = 'TESLD07T@shell.com'
  WHERE lower(type_code) in ('frm');
  COMMIT;
  DELETE
  FROM endur.user_bg_msg_delivery_details A
  WHERE ROWID >
    (SELECT MIN(rowid)
    FROM endur.user_bg_msg_delivery_details B
    WHERE A.message_delivery_id = B.message_delivery_id
    );
  UPDATE endur.user_bg_msg_delivery_details em
  SET email     = 'Endur_EOD_Notifications@shell.com'
  WHERE to_type = 'TO';
  UPDATE endur.user_bg_msg_delivery_details em
  SET email      = 'Endur_EOD_Notifications@shell.com'
  WHERE to_type IN('HOU', 'AGLNG_TVP', 'QGC', 'EGP');
  UPDATE endur.user_bg_msg_delivery_details em
  SET email = 'Endur_EOD_Notifications@shell.com'
  WHERE to_type LIKE 'SNO_AUTOSCHED%';
  COMMIT;
  UPDATE user_bg_script_config
  SET value = 'Endur_EOD_Notifications@shell.com'
  WHERE (value LIKE '%bg-group%'
  OR value LIKE '%shell%')
  AND field NOT LIKE '%from%';
  UPDATE user_bg_script_config
  SET value = 'iexplore \\nlamevs003-pub.europe.shell.com\share1\endurdata\Glossary\opencomponents_user_guide\index.html'
  WHERE (script='SHOW_OC_USER_GUIDE' and field='executable_path');
  COMMIT;
  UPDATE endur.user_dex_messaging
  SET dist_list = 'Endur_EOD_Notifications@shell.com';
  COMMIT;
CREATE USER DTPLATFM IDENTIFIED BY Shell#dtplatfm2021 DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP PROFILE APP_STD_PROFILE ACCOUNT UNLOCK;
-- 4 Roles for DTPLATFM 
GRANT CONNECT TO DTPLATFM;
GRANT OLF_READONLY TO DTPLATFM;
GRANT RESOURCE TO DTPLATFM;
GRANT SELECT_CATALOG_ROLE TO DTPLATFM;
GRANT SELECT ANY TABLE TO DTPLATFM;
ALTER USER DTPLATFM DEFAULT ROLE CONNECT,RESOURCE,SELECT_CATALOG_ROLE; 
--LNG_ENDUR_RO Account
CREATE USER LNG_ENDUR_RO IDENTIFIED BY Welcome#12 DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP PROFILE APP_ACCOUNTS ACCOUNT UNLOCK;
-- 1 Role for LNG_ENDUR_RO
GRANT OLF_USER TO LNG_ENDUR_RO;
ALTER USER LNG_ENDUR_RO DEFAULT ROLE NONE;
-- 1 System Privilege for LNG_ENDUR_RO
GRANT CREATE SESSION TO LNG_ENDUR_RO;
alter user user_endur_sdp identified by "user_endur_sdp";
UPDATE endur.job_cfg SET schedule_0 = 5 WHERE type = 0 AND service_type= 1 AND name = 'BG_BO_Endur_UserList_Monthly';
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','David.Lee6@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','Ani.A.Wu@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','Rudrashis.Ghosh2@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','Vinod-Kumar.DN@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','Naveen.Hosangadi@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','Soo-Khoon.Lee@shell.com','to');
insert into user_notify_email_address values ('BG_BO_Endur_User_List_M','c.feliciano@shell.com','to');
commit;
update endur.personnel set email = 'email ' || to_char( id_number );
update endur.personnel_history set email = 'email ' || to_char( id_number );
commit;
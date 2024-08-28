SET echo ON;
UPDATE EDW.EDW_CUBE_DB_CONFIG SET CUBE_SERVER = 'AMSDC1-S-91855\UATEU01'; 
UPDATE edw.edw_db_link_config
SET db_link_name      ='TESLD07T'
WHERE dest_system_name='ENDUR';
UPDATE edw.edw_db_link_config
SET db_link_name      ='TESLD08T'
WHERE dest_system_name='DW';
commit;
alter user dw identified by dw;
alter user edw identified by edw;
alter user dw_insert identified by dw_insert;
alter user var identified by var;
alter user dblink identified by dblink;
update global_name set global_name='TESLD08T.REGRESS.RDBMS.DEV.US.ORACLE.COM';
DROP PUBLIC DATABASE LINK "PROMHO1.REGRESS.RDBMS.DEV.US.ORACLE.COM";
DROP PUBLIC DATABASE LINK "PRTRODS1.REGRESS.RDBMS.DEV.US.ORACLE.COM";
DROP PUBLIC DATABASE LINK "PZEMHO1.REGRESS.RDBMS.DEV.US.ORACLE.COM";
DROP PUBLIC DATABASE LINK "SEDWHO1.REGRESS.RDBMS.DEV.US.ORACLE.COM";
DROP PUBLIC DATABASE LINK "TESLD01P.REGRESS.RDBMS.DEV.US.ORACLE.COM"; 
DROP PUBLIC DATABASE LINK "ENDUR.REGRESS.RDBMS.DEV.US.ORACLE.COM";
CREATE PUBLIC DATABASE LINK "ENDUR.REGRESS.RDBMS.DEV.US.ORACLE.COM"
CONNECT TO DBLINK IDENTIFIED BY dblink
USING 'TESLD07T';
DROP PUBLIC DATABASE LINK "PROLHO01.REGRESS.RDBMS.DEV.US.ORACLE.COM";
CREATE PUBLIC DATABASE LINK "TESLD07T.REGRESS.RDBMS.DEV.US.ORACLE.COM"
CONNECT TO DBLINK IDENTIFIED BY dblink
USING 'TESLD07T';
CREATE USER DTPLATFM IDENTIFIED BY WElcome_20200619 DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP PROFILE APP_STD_PROFILE;
-- 4 Roles for DTPLATFM 
GRANT CONNECT TO DTPLATFM;
GRANT OLF_READONLY TO DTPLATFM;
GRANT RESOURCE TO DTPLATFM;
GRANT SELECT_CATALOG_ROLE TO DTPLATFM;
GRANT BG_READONLY TO DTPLATFM;
ALTER USER DTPLATFM DEFAULT ROLE CONNECT,RESOURCE,SELECT_CATALOG_ROLE,BG_READONLY; 
commit;











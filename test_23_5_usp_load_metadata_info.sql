CREATE PROC [cp_data].[test_23_5_usp_load_metadata_info] 
	@project_alias NVARCHAR(30)
	, @dataset_list NVARCHAR(MAX) = ''
	, @source_list NVARCHAR(MAX) = ''
	, @load_from_processing_log int = 0
	, @processing_log_source_layer_id int = 0
	, @processing_log_target_layer_id int = 0  AS
BEGIN

	BEGIN TRY
	
		-- DECLARE @project_alias NVARCHAR(30);
		-- DECLARE @dataset_list NVARCHAR(MAX) = '';
		-- DECLARE @source_list NVARCHAR(MAX) = '';
		-- DECLARE @load_from_processing_log int = 0;
		-- DECLARE @processing_log_source_layer_id int = 0;
		-- DECLARE @processing_log_target_layer_id int = 0;
		
		-- SET @project_alias = 'cp_data'
		-- SET @processing_log_source_layer_id = 1;
		-- SET @processing_log_target_layer_id = 2;
		-- SET @load_from_processing_log = 1;
	
		DECLARE @sql_transpose_config NVARCHAR(MAX);
		DECLARE @sql_transpose_case_config NVARCHAR(MAX)  = '';
		DECLARE @sql_transpose_config_dataset NVARCHAR(MAX);
		DECLARE @sql_transpose_case_config_dataset NVARCHAR(MAX)  = '';
		DECLARE @sql_transpose_config_full NVARCHAR(MAX);
		DECLARE @status_log_query NVARCHAR(MAX);
		DECLARE @input_dataset NVARCHAR(MAX) = '';
		DECLARE @sql_transpose_watermark NVARCHAR(MAX);
	
		PRINT('Dataset list: ' + @dataset_list)
		print('Project Alias: ' + @project_alias)
	
		IF @dataset_list IS NOT NULL AND TRIM(@dataset_list) != ''
		BEGIN
			SET @dataset_list = ' AND ls.dataset IN (''' + REPLACE(REPLACE(@dataset_list, ',', ''','''), ' ', '') + ''') ';
			SET @input_dataset = @dataset_list;
		END
		ELSE
		BEGIN
			SET @dataset_list = '';
			SET @input_dataset = '';
		END
	
	
		IF @source_list IS NOT NULL AND TRIM(@source_list) != ''
		BEGIN
			SET @source_list = ' WHERE source_name IN (''' + REPLACE(REPLACE(@source_list, ',', ''','''), ' ', '') + ''') ';
		END
		ELSE
		BEGIN
			SET @source_list = '';
		END
	
		PRINT(@source_list);
		
		/**************** START - Generate CASE statements to be used in transpose query for config_dataset ****************/
		SET @sql_transpose_case_config_dataset = 	Stuff((SELECT ', '+CONCAT(' MAX(CASE WHEN config_name=''', config_name, ''' THEN config_value ELSE '''' END ) AS ', REPLACE(config_name, '?', '')) 
									FROM cp_data.config_dataset c
									JOIN cp_data.projects p ON p.project_id= c.project_id
									WHERE LOWER(p.project_alias) IN (@project_alias) AND LOWER(c.status) = 'active'
									GROUP BY c.config_name
									FOR XML PATH ('')),1,1,'');
		PRINT(@sql_transpose_case_config_dataset)
	
		IF @load_from_processing_log = 0
		BEGIN
			SET @sql_transpose_config_dataset = N' SELECT 
										cd.*
										, s.source_id
										, s.source_name
										, s.source_type
										, s.source_onprem_cloud
										, s.source_data_category
										, s.source_server
										, s.source_port_no
										, s.source_connection_type
										, s.source_database_name
										, s.source_username
										, s.source_azure_kv_secret_name
										, s.source_endpoint_base_url
										, s.source_endpoint_relative_url
										, s.source_endpoint_tenant_client_id
	
										, s.source_name_variant
										, s.source_environment
										, s.source_environment_type
									FROM (
										SELECT c.project_id AS project_id_config_dataset, ' 
											+ @sql_transpose_case_config_dataset + '
										FROM cp_data.config_dataset c
										JOIN cp_data.projects p ON p.project_id= c.project_id
										WHERE 
											LOWER(p.project_alias) IN (''' + @project_alias + ''') 
											AND LOWER(c.status) = ''active''
											AND c.source_id IN (SELECT source_id FROM cp_data.sources ' + @source_list + ' AND status=''active'' AND LOWER(project_alias) IN (''' + @project_alias + '''))
											' + REPLACE(@dataset_list, 'ls.', '') + '
										GROUP BY c.project_id, c.dataset
									) AS cd
									LEFT JOIN cp_data.sources s ON s.source_alias = cd.source_alias AND LOWER(s.status)=''active''
									' + @source_list + '
									';
		END		
		ELSE
		BEGIN
			
			IF @input_dataset IS NULL OR TRIM(@input_dataset) = ''
			BEGIN
				SET @dataset_list = Stuff((select ',' +  CONCAT(ls.source_dataset, '')
											FROM [cp_data].[load_status] ls
											JOIN cp_data.projects p ON p.project_id= ls.project_id
											WHERE	
												ls.source_layer_id=@processing_log_source_layer_id 
												AND ls.target_layer_id=@processing_log_target_layer_id
												AND LOWER(load_status) IN ('new', 'rerun')
											GROUP BY ls.source_dataset
											FOR XML PATH ('')),1,1,'');
			END
			ELSE
			BEGIN
				SET @dataset_list = @input_dataset;
			END
	
			--print(@dataset_list);
			
			IF (@input_dataset IS NULL OR TRIM(@input_dataset) = '') AND @dataset_list IS NOT NULL AND TRIM(@dataset_list) != ''
			BEGIN
				--SET @dataset_list = ' AND source_dataset IN (''' + REPLACE(REPLACE(@dataset_list, ',', ''','''), ' ', '') + ''') ';
				--SET @dataset_list = ' AND source_dataset IN (''' + REPLACE(@dataset_list, ',', ''',''') + ''') ';
				SET @dataset_list = ' AND source_dataset IN (''' + LTRIM(RTRIM(REPLACE(@dataset_list, ',', ''','''))) + ''') ';
			END
				
											
			SET @sql_transpose_config_dataset = N' SELECT 
										cd.*
										, ls_outer.source_dataset AS source_dataset
										, s.source_id
										, s.source_name
										, s.source_type
										, s.source_onprem_cloud
										, s.source_data_category
										, s.source_server
										, s.source_port_no
										, s.source_connection_type
										, s.source_database_name
										, s.source_username
										, s.source_azure_kv_secret_name
										, s.source_endpoint_base_url
										, s.source_endpoint_relative_url
										, s.source_endpoint_tenant_client_id
	
										, s.source_name_variant
										, s.source_environment
										, s.source_environment_type
										
									FROM (
										SELECT c.project_id AS project_id_config_dataset, ' 
											+ @sql_transpose_case_config_dataset + '
											, MAX(ls.load_status_id) AS load_status_id
											, MAX(ls.load_status) AS load_status
										FROM cp_data.config_dataset c
										RIGHT JOIN cp_data.load_status ls ON ls.project_id = c.project_id AND ls.dataset = c.dataset AND LOWER(ls.load_status)=''new'' AND ls.source_layer_id=' + CONVERT(varchar(5), @processing_log_source_layer_id) + ' AND ls.target_layer_id=' + CONVERT(varchar(5), @processing_log_target_layer_id) + ' AND ls.source_id IN (SELECT source_id FROM cp_data.sources ' + @source_list + ' AND status=''active'' AND LOWER(project_alias) IN (''' + @project_alias + '''))
										JOIN cp_data.projects p ON p.project_id= c.project_id
										WHERE 
											LOWER(p.project_alias) IN (''' + @project_alias + ''') 
											AND LOWER(c.status) = ''active''
											AND c.source_id IN (SELECT source_id FROM cp_data.sources ' + @source_list + ' AND status=''active'' AND LOWER(project_alias) IN (''' + @project_alias + ''')) 
											' + @dataset_list + '
										GROUP BY c.project_id, c.dataset
									) AS cd
									LEFT JOIN cp_data.sources s ON s.source_alias = cd.source_alias AND LOWER(s.status)=''active''
									LEFT JOIN cp_data.load_status ls_outer ON ls_outer.load_status_id = cd.load_status_id
									
									' + @source_list + '
									';
										--, STRING_AGG(CONCAT( CAST(cdc.source_column_name AS NVARCHAR(MAX)), '' AS '', REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cdc.source_column_name, '' '', ''_''), ''%'', ''''), ''('', ''''), '')'', ''''), ''?'', ''''), ''-'', '''') ), '', '') AS source_column_name
										--LEFT JOIN [cp_data].[config_dataset_columns] cdc ON cdc.project_id=p.project_id AND cdc.source_id=ls.source_id AND cdc.dataset=ls.dataset
										
		END					
		
		print('Query 2: ' + @sql_transpose_config_dataset)
		
		-- SET @sql_transpose_config_dataset = N'SELECT c.project_id AS project_id_config_dataset, dataset
		-- 							FROM cp_data.config_dataset c
		-- 							LEFT JOIN cp_data.projects p ON p.project_id= c.project_id
		-- 							WHERE 
		--  								LOWER(p.project_alias) IN (''' + @project_alias + ''') 
		-- 								AND LOWER(c.status) = ''active''
		-- 						v		' + @dataset_list + '';
		-- PRINT(@sql_transpose_config_dataset)
		-- EXEC(@sql_transpose_config_dataset)
		/**************** END - Generate CASE statements to be used in transpose query for config_dataset ****************/
	
		
		
		/**************** START - Generate CASE statements to be used in transpose query for config_dataset ****************/
		SET @sql_transpose_case_config = 	Stuff((SELECT ', '+CONCAT(' MAX(CASE WHEN config_name=''', config_name, ''' THEN config_value ELSE '''' END ) AS ', REPLACE(config_name, '?', '')) 
									FROM cp_data.config c
									JOIN cp_data.projects p ON p.project_id= c.project_id
									WHERE LOWER(p.project_alias) IN (@project_alias) AND LOWER(c.status) = 'active'
									GROUP BY c.config_name
									FOR XML PATH ('')),1,1,'');
		--PRINT(@sql_transpose_case_config)
		
			
		-- SET @sql_transpose_config = N' SELECT c.project_id, w.last_load_value, ' + @sql_transpose_case_config + '
		-- 	FROM cp_data.config c
		-- 	JOIN cp_data.projects p ON p.project_id= c.project_id
		-- 	LEFT JOIN cp_data.watermark w ON p.project_id= w.project_id AND c.dataset= w.dataset
		-- 	WHERE 
		--  		LOWER(p.project_alias) IN (''' + @project_alias + ''') AND LOWER(c.status) = ''active''
		-- 	GROUP BY c.project_id';
	
		SET @sql_transpose_config = N' SELECT c.project_id, ' + @sql_transpose_case_config + '
			FROM cp_data.config c
			JOIN cp_data.projects p ON p.project_id= c.project_id
			WHERE 
				LOWER(p.project_alias) IN (''' + @project_alias + ''') AND LOWER(c.status) = ''active''
			GROUP BY c.project_id';
		--EXEC(@sql_transpose_config)
		
		/**************** END - Generate CASE statements to be used in transpose query for config_dataset ****************/
												
	
		SET @sql_transpose_watermark = 'SELECT a.project_id ,a.source_id ,a.source_schema ,a.dataset ,max(a.last_load_value) as last_load_value
										FROM (SELECT * , dense_rank() over (partition by project_id ,source_id ,source_schema ,dataset  order by watermark_id desc) as rn
										FROM cp_data.watermark
										WHERE watermark_status = ''COMPLETE''
										) a
										WHERE a.rn = 1
										GROUP BY a.project_id ,a.source_id ,a.source_schema ,a.dataset
									   ';
		SET @sql_transpose_config_full = 'SELECT full_conf.*, w.last_load_value FROM
											(
												SELECT * FROM (' 
													+ @sql_transpose_config + 
												') AS conf	
												JOIN (' + @sql_transpose_config_dataset + ') AS conf_dataset
												ON conf.project_id = conf_dataset.project_id_config_dataset
											) AS full_conf
											LEFT JOIN ('+ @sql_transpose_watermark +'	) w
											ON full_conf.project_id = w.project_id
											AND full_conf.source_id = w.source_id
											AND full_conf.dataset = w.dataset
											AND full_conf.config_source_schema = w.source_schema
										 ';
	
		--PRINT @sql_transpose_watermark
		--PRINT('Query Final : ' + @sql_transpose_config_full);
		EXEC(@sql_transpose_config_full);
	
	END TRY 
	
	BEGIN CATCH
		Declare @ErrorMessage nvarchar(4000);
		Declare @ErrorSeverity int;
	
		SELECT @ErrorMessage = error_message(), @ErrorSeverity = error_severity();
		RAISERROR(@ErrorMessage, @ErrorSeverity, 1);
	
	END CATCH

END
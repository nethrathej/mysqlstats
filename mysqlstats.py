#!/usr/bin/python
from __future__ import print_function
import sys
import logging
import pymysql
import csv
import json
import boto3
import os
import uuid
import time

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)


db_name = "<aurora mysql database name>"
clientdet = boto3.client('rds',region_name='<your region>')
token = clientdet.generate_db_auth_token('<rds proxy endpoint>',<aurora database port number>,'<db user>')
ssl = {'ca':'AmazonRootCA1.pem'}
auth_plugin_map = {'plugin_name':'mysql_clear_password'}


try:
    conn = pymysql.connect(host='<rds proxy endpoint>',database='<aurora mysql database name>',port=<aurora mysql db port number>,user='<db user>',password=token,ssl=ssl,auth_plugin_map=auth_plugin_map)

except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

def lambda_handler(event, context):

    """
    This function gathers monitoring details from mysql RDS/Aurora instance
    """
    t = time.strftime('%Y%m%dT%H%M%S')
    cur=conn.cursor()
    header10=["Current TimeStamp"]
    cur.execute("SELECT CURRENT_TIMESTAMP();")
    result10=cur.fetchall()
    line10=["id","user","host","db","command","time","state","info"]
    header1=["Process List"]
    cur.execute("SELECT PROCESSLIST_ID AS id, PROCESSLIST_USER AS user,PROCESSLIST_HOST AS host,PROCESSLIST_DB AS db,PROCESSLIST_COMMAND AS command,PROCESSLIST_TIME AS time,PROCESSLIST_STATE AS state,LEFT(PROCESSLIST_INFO, 80) AS info FROM performance_schema.threads WHERE PROCESSLIST_ID IS NOT NULL AND PROCESSLIST_COMMAND NOT IN ('Sleep', 'Binlog Dump') ORDER BY PROCESSLIST_TIME ASC")
    result1=cur.fetchall()
    line1=["id","user","host","db","command","time","state","info"]
    header3=["Current Event Statements"]
    cur.execute("select * from performance_schema.events_statements_current")
    result3=cur.fetchall()
    line3=["THREAD_ID","EVENT_ID","END_EVENT_ID","EVENT_NAME","SOURCE","TIMER_START","TIMER_END","TIMER_WAIT","LOCK_TIME","SQL_TEXT","DIGEST","DIGEST_TEXT","CURRENT_SCHEMA","OBJECT_TYPE","OBJECT_SCHEMA","OBJECT_NAME","OBJECT_INSTANCE_BEGIN","MYSQL_ERRNO","RETURNED_SQLSTATE","MESSAGE_TEXT","ERRORS","WARNINGS","ROWS_AFFECTED","ROWS_SENT","ROWS_EXAMINED","CREATED_TMP_DISK_TABLES","CREATED_TMP_TABLES","SELECT_FULL_JOIN","SELECT_FULL_RANGE_JOIN","SELECT_RANGE","SELECT_RANGE_CHECK","SELECT_SCAN","SORT_MERGE_PASSES","SORT_RANGE","SORT_ROWS","SORT_SCAN","NO_INDEX_USED","NO_GOOD_INDEX_USED","NESTING_EVENT_ID","NESTING_EVENT_TYPE","NESTING_EVENT_LEVEL"]
    header4=["Current Event Waits"]
    cur.execute("select * from performance_schema.events_waits_current")
    result4=cur.fetchall()
    line4=["THREAD_ID","EVENT_ID","END_EVENT_ID","EVENT_NAME","SOURCE","TIMER_START","TIMER_END","TIMER_WAIT","SPINS","OBJECT_SCHEMA","OBJECT_NAME","INDEX_NAME","OBJECT_TYPE","OBJECT_INSTANCE_BEGIN","NESTING_EVENT_ID","NESTING_EVENT_TYPE","OPERATION","NUMBER_OF_BYTES","FLAGS"]
    header5=["History Event Waits"]
    cur.execute("select * from performance_schema.events_waits_history")
    result5=cur.fetchall()
    line5=["THREAD_ID","EVENT_ID","END_EVENT_ID","EVENT_NAME","SOURCE","TIMER_START","TIMER_END","TIMER_WAIT","SPINS","OBJECT_SCHEMA","OBJECT_NAME","INDEX_NAME","OBJECT_TYPE","OBJECT_INSTANCE_BEGIN","NESTING_EVENT_ID","NESTING_EVENT_TYPE","OPERATION","NUMBER_OF_BYTES","FLAGS"]
    header6=["Top 5 Wait Events"]
    cur.execute("SELECT * FROM performance_schema.events_waits_summary_global_by_event_name WHERE count_star>0 ORDER BY count_star DESC LIMIT 5")
    result6=cur.fetchall()
    line6=["EVENT_NAME","COUNT_STAR","SUM_TIMER_WAIT","MIN_TIMER_WAIT","AVG_TIMER_WAIT","MAX_TIMER_WAIT"]
    header7=["Top 10 Longest Wait"]
    cur.execute("select event_name as wait_event, count_star as all_occurrences, CONCAT(ROUND(sum_timer_wait / 1000000000000, 2), ' s') as total_wait_time, CONCAT(ROUND(avg_timer_wait / 1000000000, 2), ' ms') as avg_wait_time from performance_schema.events_waits_summary_global_by_event_name where count_star > 0 and event_name <> 'idle' order by sum_timer_wait desc limit 10")
    result7=cur.fetchall()
    line7=["WAIT_EVENT","ALL_OCCURANCES","TOTAL_WAIT_TIME","AVG_WAIT_TIME"]
    header8=["Top 10 Statements by order by total execution time"]
    cur.execute("select replace(event_name, 'statement/sql/', '') as statement, count_star as all_occurrences, CONCAT(ROUND(sum_timer_wait / 1000000000000, 2), ' s') as total_latency, CONCAT(ROUND(avg_timer_wait / 1000000000000, 2), ' s') as avg_latency, CONCAT(ROUND(sum_lock_time / 1000000000000, 2), ' s') as total_lock_time, sum_rows_affected as sum_rows_changed, sum_rows_sent as sum_rows_selected, sum_rows_examined as sum_rows_scanned, sum_created_tmp_tables, sum_created_tmp_disk_tables, if(sum_created_tmp_tables = 0, 0, concat(truncate(sum_created_tmp_disk_tables/sum_created_tmp_tables*100, 0))) as tmp_disk_tables_percent, sum_select_scan, sum_no_index_used, sum_no_good_index_used from performance_schema.events_statements_summary_global_by_event_name where event_name like 'statement/sql/%' and count_star > 0 order by sum_timer_wait desc limit 10")
    result8=cur.fetchall()
    line8=["STATEMENT","ALL_OCCURANCES","TOTAL_LATENCY","AVG_LATENCY","TOTAL_LOCK_TIME","SUM_ROWS_CHANGED","SUM_ROWS_SELECTED","SUM_ROWS_SCANNED","SUM_CREATED_TMP_TABLES","SUM_CREATED_TMP_DISK_TABLES","TMP_DISK_TABLE_PERCENT","SUM_SELECT_SCAN","SUM_NO_INDEX_USED","SUM_NO_GOOD_INDEX_USED"]
    header9=["Top 10 Statements by order by number of executions"]
    cur.execute("select replace(event_name, 'statement/sql/', '') as statement, count_star as all_occurrences, CONCAT(ROUND(sum_timer_wait / 1000000000000, 2), ' s') as total_latency, CONCAT(ROUND(avg_timer_wait / 1000000000000, 2), ' s') as avg_latency, CONCAT(ROUND(sum_lock_time / 1000000000000, 2), ' s') as total_lock_time, sum_rows_affected as sum_rows_changed, sum_rows_sent as sum_rows_selected, sum_rows_examined as sum_rows_scanned, sum_created_tmp_tables, sum_created_tmp_disk_tables, if(sum_created_tmp_tables = 0, 0, concat(truncate(sum_created_tmp_disk_tables/sum_created_tmp_tables*100, 0))) as tmp_disk_tables_percent, sum_select_scan, sum_no_index_used, sum_no_good_index_used from performance_schema.events_statements_summary_global_by_event_name where event_name like 'statement/sql/%' and count_star > 0 order by count_star desc limit 10")
    result9=cur.fetchall()
    line9=["STATEMENT","ALL_OCCURANCES","TOTAL_LATENCY","AVG_LATENCY","TOTAL_LOCK_TIME","SUM_ROWS_CHANGED","SUM_ROWS_SELECTED","SUM_ROWS_SCANNED","SUM_CREATED_TMP_TABLES","SUM_CREATED_TMP_DISK_TABLES","TMP_DISK_TABLE_PERCENT","SUM_SELECT_SCAN","SUM_NO_INDEX_USED","SUM_NO_GOOD_INDEX_USED"]
    header11=["Top 10 Statements by order by number of executions"]
    cur.execute("select r.trx_id AS waiting_trx_id, r.trx_mysql_thread_id AS waiting_thread, r.trx_query AS waiting_query, rl.lock_id AS waiting_lock_id, rl.lock_mode AS waiting_lock_mode, rl.lock_type AS waiting_lock_type, rl.lock_table AS waiting_lock_table, rl.lock_index AS waiting_lock_index, b.trx_id AS blocking_trx_id, b.trx_mysql_thread_id AS blocking_thread, b.trx_query AS blocking_query, bl.lock_id AS blocking_lock_id, bl.lock_mode AS blocking_lock_mode, bl.lock_type AS blocking_lock_type, bl.lock_table AS blocking_lock_table, bl.lock_index AS blocking_lock_index from ((((information_schema.INNODB_LOCK_WAITS w join information_schema.INNODB_TRX b on((b.trx_id = w.blocking_trx_id))) join information_schema.INNODB_TRX r on((r.trx_id = w.requesting_trx_id))) join information_schema.INNODB_LOCKS bl on((bl.lock_id = w.blocking_lock_id))) join information_schema.INNODB_LOCKS rl on((rl.lock_id = w.requested_lock_id)))")
    result11=cur.fetchall()
    line11=["WAITING_TRX_ID","WAITING_THREAD","WAITING_QUERY","WAITING_LOCK_ID","WAITING_LOCK_MODE","WAITING_LOCK_TYPE","WAITING_LOCK_TABLE","WAITING_LOCK_INDEX","BLOCKING_TRX_ID","BLOCKING_THREAD","BLOCKING_QUERY","BLOCKING_LOCK_ID","BLOCKING_LOCK_MODE","BLOCKING_LOCK_TYPE","BLOCKING_LOCK_TABLE","BLOCKING_LOCK_INDEX"]
    header12=["Busiest indexes on the server"]
    cur.execute("SELECT object_schema as table_schema, object_name as table_name, index_name, count_star as all_accesses, count_read, count_write, concat(truncate(count_read/count_star*100, 0),':',truncate(count_write/count_star*100,0)) as read_write_ratio, count_fetch as rows_selected, count_insert as rows_inserted, count_update as rows_updated, count_delete as rows_deleted, CONCAT(ROUND(sum_timer_wait / 1000000000000, 2), ' s') as total_latency, CONCAT(ROUND(sum_timer_fetch / 1000000000000, 2), ' s') as select_latency, CONCAT(ROUND(sum_timer_insert / 1000000000000, 2), ' s') as insert_latency, CONCAT(ROUND(sum_timer_update / 1000000000000, 2), ' s') as update_latency, CONCAT(ROUND(sum_timer_delete / 1000000000000, 2), ' s') as delete_latency FROM performance_schema.table_io_waits_summary_by_index_usage WHERE index_name IS NOT NULL and count_star > 0 ORDER BY sum_timer_wait DESC")
    result12=cur.fetchall()
    line12=["TABLE_SCHEMA","TABLE_NAME","INDEX_NAME","ALL_ACCESSES","COUNT_READ","COUNT_WRITE","READ_WRITE_RATIO","ROWS_SELECTED","ROWS_INSERTED","ROWS_UPDATED","ROWS_DELETED","TOTAL_LATENCY","SELECT_LATENCY","INSERT_LATENCY","UPDATE_LATENCY","DELETE_LATENCY"]
    header2=["INNODB Status"]
    cur.execute("SHOW ENGINE INNODB STATUS")
    result2=cur.fetchall()


    file_name1 = db_name + "_mysqlperf_" + t + ".csv"
    lambda_path1 = "/tmp/" + file_name1
    s3_path1 = file_name1

    with open(lambda_path1,'w') as fileout:
        writer = csv.writer(fileout)
        writer.writerow(header10)
        writer.writerows(result10)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header1)
        writer.writerow(line1)
        writer.writerows(result1)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header3)
        writer.writerow(line3)
        writer.writerows(result3)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header4)
        writer.writerow(line4)
        writer.writerows(result4)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header5)
        writer.writerow(line5)
        writer.writerows(result5)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header6)
        writer.writerow(line6)
        writer.writerows(result6)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header7)
        writer.writerow(line7)
        writer.writerows(result7)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header8)
        writer.writerow(line8)
        writer.writerows(result8)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header9)
        writer.writerow(line9)
        writer.writerows(result9)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header11)
        writer.writerow(line11)
        writer.writerows(result11)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header12)
        writer.writerow(line12)
        writer.writerows(result12)
        writer.writerow('\n')
        writer.writerow('\n')
        writer.writerow(header2)
        writer.writerow(result2)
        writer.writerow('\n')
        writer.writerow('\n')

    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(lambda_path1, '<s3 bucket name>', s3_path1)

    return

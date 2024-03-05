# Author       : Deepika S
# CreatedDate  : 05-03-2023
# Updated Date :
# Changes Done :

""" This script will refresh the redshift materialized views for iSOC-revamp-snapchat"""

from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from config import global_config


class RefreshData:

    def __init__(self):
        load_dotenv()
        self.global_config = global_config.GlobalConfig()
        self.secret_key = self.global_config.secret_key
        # Connection to iSOCRATES redshift
        self.redshift_con = psycopg2.connect(dbname=self.secret_key['REDSHIFT_DBNAME'],
                                             host=self.secret_key['REDSHIFT_HOST'],
                                             port=self.secret_key['REDSHIFT_PORT'],
                                             user=self.secret_key['REDSHIFT_USER'],
                                             password=self.secret_key['REDSHIFT_PASSWORD'])
        self.redshift_cur = self.redshift_con.cursor()

        self.redshift_cur = self.redshift_con.cursor()
        self.views = [ "mat_isoc_revamp_network_analytics_report",
#                      "mat_isoc_revamp_network_analytics_report_temp",
                      "mat_isoc_revamp_network_video_analytics_report",
#                      "mat_isoc_revamp_network_video_analytics_report_temp",
                      "mat_isoc_revamp_network_site_domain_performance_report",
#                      "mat_isoc_revamp_network_site_domain_performance_report_temp",
                      "mat_isoc_revamp_network_geo_analytics_report",
#                      "mat_isoc_revamp_network_geo_analytics_report_temp"
                      "mat_isoc_revamp_network_device_analytics_report",
#                      "mat_isoc_revamp_network_device_analytics_report_temp",
                      "mat_isoc_revamp_network_buyer_data_usage_analytics_report",
#                      "mat_isoc_revamp_network_buyer_data_usage_analytics_report_temp",
#                      "mat_isoc_revamp_xandr_insertion_order_dim","mat_isoc_revamp_xandr_line_item_dim","mat_isoc_revamp_xandr_creative_dim","mat_isoc_revamp_xandr_advertiser_dim","mat_isoc_revamp_xandr_billing_period_dim","mat_isoc_revamp_xandr_campaign_group_dim","mat_isoc_revamp_xandr_campaign_dim","mat_isoc_revamp_xandr_site_dim","mat_isoc_revamp_xandr_split_dim",
                      "mat_isoc_revamp_network_reports_all",
                      "mat_isoc_dimdates"]

    def start(self):
        query = "REFRESH MATERIALIZED VIEW {}".format(self.secret_key['REDSHIFT_SCHEMA'])
        for table in self.views:
            print(table)
            print(datetime.now())
            query_string = '{}.{}'.format(query, table)
            self.redshift_cur.execute(query_string)
            self.redshift_con.commit()
            print(datetime.now())


obj = RefreshData()
obj.start()


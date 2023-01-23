import json
from aws_cdk import (aws_s3 as s3, aws_glue_alpha as glue)
import aws_cdk as cdk
from constructs import Construct


class ChargebackAthenaStack(cdk.Stack):
	def __init__(self, scope: Construct, construct_id: str,raw_config,**kwargs):
		super().__init__(scope,construct_id, **kwargs)

		glue_database = glue.Database(self, raw_config.athena_info["database_name"],
							database_name = raw_config.athena_info["database_name"]
						)
		common_s3_bucket = s3.Bucket.from_bucket_arn(self,
					id= raw_config.athena_info["common_bucket"],
					bucket_arn = "arn:aws:s3:::"+raw_config.athena_info["common_bucket"]
					)
				

		glue_mvr_table = glue.Table(
			self, raw_config.athena_info["mvr_table_name"],
			database=glue_database,
			table_name=raw_config.athena_info["mvr_table_name"],
			bucket = common_s3_bucket,
			s3_prefix=raw_config.athena_info["mvr_table_s3_prefix"],
			columns=[
				glue.Column(
					name="producerCode",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="lob",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="baseState",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="quoteID",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="chargebackamount",
					type=glue.Schema.DOUBLE
				),
				glue.Column(
					name="totalDrivers",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="totalOrderedDrivers",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="orderedDriversForCurrReq",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="costPerReport",
					type=glue.Schema.DOUBLE
				),
				glue.Column(
					name="firstOrderDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="cycleCloseDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="currenttimestamp",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="startDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="reportType",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="orderDate",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="quoteDate",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="policyIssued",
					type=glue.Schema.BOOLEAN
				),
				glue.Column(
					name="policyIssueDate",
					type=glue.Schema.TIMESTAMP
				),
                glue.Column(
					name="policyNumber",
					type=glue.Schema.STRING
				),
                glue.Column(
					name="correlationId",
					type=glue.Schema.STRING
				),
                glue.Column(
					name="shallOrderFlag",
					type=glue.Schema.BOOLEAN
				),
                glue.Column(
					name="totalCharge",
					type=glue.Schema.DOUBLE
				)
				],
			data_format=glue.DataFormat(input_format=glue.InputFormat.TEXT,
						output_format = glue.OutputFormat.HIVE_IGNORE_KEY_TEXT,
						serialization_library = glue.SerializationLibrary.PARQUET,
			),
			partition_keys = [glue.Column(
					name="ingestion_yyyymmdd",
					type=glue.Schema.INTEGER,
				)]
		)

		glue_clue_table = glue.Table(
			self, raw_config.athena_info["clue_table_name"],
			database=glue_database,
			table_name=raw_config.athena_info["clue_table_name"],
			bucket = common_s3_bucket,
			s3_prefix=raw_config.athena_info["clue_table_s3_prefix"],
			columns=[
				glue.Column(
					name="producerCode",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="lob",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="baseState",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="quoteID",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="chargebackamount",
					type=glue.Schema.DOUBLE
				),
				glue.Column(
					name="totalDrivers",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="totalOrderedDrivers",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="orderedDriversForCurrReq",
					type=glue.Schema.INTEGER
				),
				glue.Column(
					name="costPerReport",
					type=glue.Schema.DOUBLE
				),
				glue.Column(
					name="firstOrderDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="cycleCloseDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="currenttimestamp",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="startDate",
					type=glue.Schema.DATE
				),
				glue.Column(
					name="reportType",
					type=glue.Schema.STRING
				),
				glue.Column(
					name="orderDate",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="quoteDate",
					type=glue.Schema.TIMESTAMP
				),
				glue.Column(
					name="policyIssued",
					type=glue.Schema.BOOLEAN
				),
				glue.Column(
					name="policyIssueDate",
					type=glue.Schema.TIMESTAMP
				),
                glue.Column(
					name="policyNumber",
					type=glue.Schema.STRING
				),
                glue.Column(
					name="correlationId",
					type=glue.Schema.STRING
				),
                glue.Column(
					name="shallOrderFlag",
					type=glue.Schema.BOOLEAN
				),
                glue.Column(
					name="totalCharge",
					type=glue.Schema.DOUBLE
				)
				],
			data_format=glue.DataFormat(input_format=glue.InputFormat.TEXT,
						output_format = glue.OutputFormat.HIVE_IGNORE_KEY_TEXT,
						serialization_library = glue.SerializationLibrary.PARQUET,
			),
			partition_keys = [glue.Column(
					name="ingestion_yyyymmdd",
					type=glue.Schema.INTEGER,
				)]
		)